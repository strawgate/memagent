//! SQL query analysis and predicate-hint extraction.

use std::collections::HashSet;

use datafusion::sql::sqlparser::ast::{
    self as sqlast, Expr as SqlExpr, SelectItem, SetExpr, Statement, WildcardAdditionalOptions,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use logfwd_core::scan_config::ScanConfig;

use crate::TransformError;

/// Parses SQL at startup, extracts column references and determines scan config.
pub struct QueryAnalyzer {
    /// User-provided SQL text.
    pub user_sql: String,
    /// Referenced columns collected from SELECT/WHERE/GROUP BY/HAVING/ORDER BY.
    pub referenced_columns: HashSet<String>,
    /// Whether the query projection contains `*` or a qualified wildcard.
    pub uses_select_star: bool,
    /// Fields listed in a `SELECT * EXCEPT (...)` clause.
    pub except_fields: Vec<String>,
    /// The WHERE clause AST, if present. Used for predicate pushdown extraction.
    where_clause: Option<SqlExpr>,
}

impl QueryAnalyzer {
    /// Parse the SQL and extract metadata about column usage.
    pub fn new(sql: &str) -> Result<Self, TransformError> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| TransformError::Sql(format!("SQL parse error: {e}")))?;

        if statements.len() != 1 {
            return Err(TransformError::Sql(
                "Expected exactly one SQL statement".to_string(),
            ));
        }

        let stmt = &statements[0];
        let mut referenced_columns = HashSet::new();
        let mut uses_select_star = false;
        let mut except_fields = Vec::new();
        let mut where_clause = None;

        if let Statement::Query(query) = stmt {
            if let SetExpr::Select(select) = query.body.as_ref() {
                for item in &select.projection {
                    match item {
                        SelectItem::Wildcard(opts) => {
                            uses_select_star = true;
                            extract_except_fields(opts, &mut except_fields);
                        }
                        SelectItem::QualifiedWildcard(_, opts) => {
                            uses_select_star = true;
                            extract_except_fields(opts, &mut except_fields);
                        }
                        SelectItem::UnnamedExpr(expr) => {
                            collect_column_refs(expr, &mut referenced_columns);
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            collect_column_refs(expr, &mut referenced_columns);
                        }
                    }
                }

                // Walk WHERE clause for column references.
                if let Some(ref selection) = select.selection {
                    collect_column_refs(selection, &mut referenced_columns);
                    where_clause = Some(selection.clone());
                }

                // Walk GROUP BY — columns may appear only here (not in SELECT or WHERE).
                if let sqlast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                    for e in exprs {
                        collect_column_refs(e, &mut referenced_columns);
                    }
                }

                // Walk HAVING — HAVING MAX(col) > N where col is not in SELECT.
                if let Some(ref having) = select.having {
                    collect_column_refs(having, &mut referenced_columns);
                }

                // Walk WINDOW named definitions — PARTITION BY / ORDER BY
                // columns may only appear in named window specs.
                for sqlast::NamedWindowDefinition(_, named_expr) in &select.named_window {
                    if let sqlast::NamedWindowExpr::WindowSpec(spec) = named_expr {
                        for e in &spec.partition_by {
                            collect_column_refs(e, &mut referenced_columns);
                        }
                        for ob in &spec.order_by {
                            collect_column_refs(&ob.expr, &mut referenced_columns);
                        }
                    }
                }
            }

            // Walk ORDER BY — columns may appear only here (not in SELECT or WHERE).
            if let Some(ref order_by) = query.order_by {
                if let sqlast::OrderByKind::Expressions(exprs) = &order_by.kind {
                    for ob in exprs {
                        collect_column_refs(&ob.expr, &mut referenced_columns);
                    }
                }
            }
        } else {
            return Err(TransformError::Sql(
                "Only SELECT statements are supported".to_string(),
            ));
        }

        Ok(QueryAnalyzer {
            user_sql: sql.to_string(),
            referenced_columns,
            uses_select_star,
            except_fields,
            where_clause,
        })
    }

    /// Generate ScanConfig for the scanner based on query analysis.
    ///
    /// SQL column references use bare field names (`level`, `status`).
    /// Conflict fields are now emitted as struct columns by the builders, so
    /// there are no `__int`/`__str`/`__float` suffixed names in SQL.
    /// `strip_type_suffix` is a no-op and is kept only for call-site symmetry.
    pub fn scan_config(&self) -> ScanConfig {
        // Enable keep_raw only when the query explicitly references the `_raw`
        // column.  For `SELECT *` we deliberately do NOT force keep_raw: the
        // pipeline already enables it for `format: raw` inputs (where every
        // line must be captured), and for `format: json` / `format: cri`
        // inputs the raw bytes are redundant — forcing keep_raw on `SELECT *`
        // doubles memory use (documented at ~65 % overhead) without any
        // user opt-in.
        let keep_raw = self.referenced_columns.contains("_raw");

        if self.uses_select_star {
            ScanConfig {
                wanted_fields: vec![],
                extract_all: true,
                keep_raw,
                validate_utf8: false,
            }
        } else {
            use logfwd_core::scan_config::FieldSpec;
            use std::collections::HashSet;
            // `_raw` is not a JSON field but a special scanner-provided column.
            // When it is referenced in the SQL (e.g. `json(_raw, 'key')`), the
            // scanner must include it in the output batch.  Strip it from the
            // `wanted_fields` list (it is not a JSON key) and enable `keep_raw`
            // instead (#1627).  `keep_raw` is already computed above.
            let mut seen = HashSet::new();
            let wanted: Vec<FieldSpec> = self
                .referenced_columns
                .iter()
                .filter(|name| *name != "_raw") // _raw is not a JSON key
                .map(|name| strip_type_suffix(name))
                .filter(|name| seen.insert(name.clone()))
                .map(|name| FieldSpec {
                    name,
                    aliases: vec![],
                })
                .collect();
            ScanConfig {
                wanted_fields: wanted,
                extract_all: false,
                keep_raw,
                validate_utf8: false,
            }
        }
    }

    /// Extract filter hints from the SQL for predicate pushdown.
    ///
    /// Walks the WHERE clause looking for simple predicates on known syslog
    /// columns (severity, facility) that can be pushed to input sources.
    /// Only predicates in top-level AND chains are extracted — OR'd predicates
    /// are left for DataFusion since pushing them could miss matching rows.
    pub fn filter_hints(&self) -> logfwd_types::filter_hints::FilterHints {
        let mut hints = logfwd_types::filter_hints::FilterHints::default();

        if let Some(ref where_expr) = self.where_clause {
            extract_pushable_predicates(where_expr, &mut hints);
        }

        hints.wanted_fields = if self.uses_select_star {
            None
        } else {
            Some(
                self.referenced_columns
                    .iter()
                    .filter(|name| name.as_str() != "_raw")
                    .map(|c| strip_type_suffix(c))
                    .collect(),
            )
        };

        hints
    }
}

/// Walk a WHERE clause AST and extract predicates that can be pushed down.
/// Only extracts from top-level AND chains (not OR branches).
fn extract_pushable_predicates(
    expr: &SqlExpr,
    hints: &mut logfwd_types::filter_hints::FilterHints,
) {
    match expr {
        // AND: recurse into both sides
        SqlExpr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::And,
            right,
        } => {
            extract_pushable_predicates(left, hints);
            extract_pushable_predicates(right, hints);
        }
        // severity <= N, severity < N, severity = N
        SqlExpr::BinaryOp { left, op, right } => {
            if let Some(col) = expr_as_column(left) {
                let col_base = strip_type_suffix(&col);
                if let Some(val) = expr_as_u8_literal(right) {
                    match (col_base.as_str(), op) {
                        ("severity", sqlast::BinaryOperator::LtEq) => {
                            tighten_max_severity(&mut hints.max_severity, val);
                        }
                        ("severity", sqlast::BinaryOperator::Lt) if val > 0 => {
                            tighten_max_severity(&mut hints.max_severity, val - 1);
                        }
                        ("severity", sqlast::BinaryOperator::Eq) => {
                            tighten_max_severity(&mut hints.max_severity, val);
                        }
                        ("facility", sqlast::BinaryOperator::Eq) => {
                            tighten_facilities(&mut hints.facilities, vec![val]);
                        }
                        _ => {}
                    }
                }
            }
            // Also handle N <= severity (reversed operand order)
            if let Some(col) = expr_as_column(right) {
                let col_base = strip_type_suffix(&col);
                if let Some(val) = expr_as_u8_literal(left) {
                    match (col_base.as_str(), op) {
                        ("severity", sqlast::BinaryOperator::GtEq) => {
                            tighten_max_severity(&mut hints.max_severity, val);
                        }
                        ("severity", sqlast::BinaryOperator::Gt) if val > 0 => {
                            tighten_max_severity(&mut hints.max_severity, val - 1);
                        }
                        _ => {}
                    }
                }
            }
        }
        // facility IN (1, 4, 16)
        SqlExpr::InList {
            expr,
            list,
            negated: false,
        } => {
            if let Some(col) = expr_as_column(expr) {
                let col_base = strip_type_suffix(&col);
                if col_base == "facility" {
                    let vals: Vec<u8> = list.iter().filter_map(expr_as_u8_literal).collect();
                    if !vals.is_empty() && vals.len() == list.len() {
                        tighten_facilities(&mut hints.facilities, vals);
                    }
                }
            }
        }
        // Parenthesized expression
        SqlExpr::Nested(inner) => {
            extract_pushable_predicates(inner, hints);
        }
        // OR, complex expressions — don't push (might miss rows)
        _ => {}
    }
}

/// Tighten severity bound: keep the minimum of existing and new.
fn tighten_max_severity(slot: &mut Option<u8>, candidate: u8) {
    *slot = Some(slot.map_or(candidate, |cur| cur.min(candidate)));
}

/// Tighten facility set: intersect with existing, or set if first time.
fn tighten_facilities(slot: &mut Option<Vec<u8>>, candidate: Vec<u8>) {
    match slot {
        Some(current) => current.retain(|f| candidate.contains(f)),
        None => *slot = Some(candidate),
    }
}

/// Extract column name from an identifier expression.
fn expr_as_column(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => Some(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

/// Extract a small integer literal from a SQL expression.
fn expr_as_u8_literal(expr: &SqlExpr) -> Option<u8> {
    match expr {
        SqlExpr::Value(v) => match &v.value {
            sqlast::Value::Number(s, _) => s.parse::<u8>().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// Strip a conflict-column type suffix from a column name.
///
/// Struct conflict format has no `__int`/`__str`/`__float` suffixes; this
/// function is now a no-op kept only so call sites remain unchanged.
fn strip_type_suffix(name: &str) -> String {
    // Struct conflict format has no type suffixes; this function is now a no-op.
    name.to_string()
}

/// Extract EXCEPT field names from wildcard options.
fn extract_except_fields(opts: &WildcardAdditionalOptions, out: &mut Vec<String>) {
    if let Some(ref except) = opts.opt_except {
        out.push(except.first_element.value.clone());
        for ident in &except.additional_elements {
            out.push(ident.value.clone());
        }
    }
}

/// Recursively collect column name references from a SQL expression.
fn collect_column_refs(expr: &SqlExpr, cols: &mut HashSet<String>) {
    match expr {
        SqlExpr::Identifier(ident) => {
            cols.insert(ident.value.clone());
        }
        SqlExpr::CompoundIdentifier(parts) => {
            // e.g. logs.field — take the last part as the column name.
            if let Some(last) = parts.last() {
                cols.insert(last.value.clone());
            }
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_column_refs(left, cols);
            collect_column_refs(right, cols);
        }
        SqlExpr::UnaryOp { expr, .. } => {
            collect_column_refs(expr, cols);
        }
        SqlExpr::Function(func) => {
            match &func.args {
                sqlast::FunctionArguments::List(arg_list) => {
                    for arg in &arg_list.args {
                        match arg {
                            sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) => {
                                collect_column_refs(e, cols);
                            }
                            sqlast::FunctionArg::Named {
                                arg: sqlast::FunctionArgExpr::Expr(e),
                                ..
                            } => {
                                collect_column_refs(e, cols);
                            }
                            _ => {}
                        }
                    }
                }
                sqlast::FunctionArguments::None => {}
                sqlast::FunctionArguments::Subquery(_) => {}
            }
            // Walk OVER clause: columns in PARTITION BY and ORDER BY are
            // only referenced in func.over, not in func.args.
            if let Some(sqlast::WindowType::WindowSpec(spec)) = &func.over {
                for e in &spec.partition_by {
                    collect_column_refs(e, cols);
                }
                for ob in &spec.order_by {
                    collect_column_refs(&ob.expr, cols);
                }
            }
        }
        SqlExpr::Nested(inner) => {
            collect_column_refs(inner, cols);
        }
        SqlExpr::IsNull(e)
        | SqlExpr::IsNotNull(e)
        | SqlExpr::IsTrue(e)
        | SqlExpr::IsNotTrue(e)
        | SqlExpr::IsFalse(e)
        | SqlExpr::IsNotFalse(e)
        | SqlExpr::IsUnknown(e)
        | SqlExpr::IsNotUnknown(e) => {
            collect_column_refs(e, cols);
        }
        SqlExpr::IsDistinctFrom(left, right) | SqlExpr::IsNotDistinctFrom(left, right) => {
            collect_column_refs(left, cols);
            collect_column_refs(right, cols);
        }
        SqlExpr::SimilarTo { expr, pattern, .. } => {
            collect_column_refs(expr, cols);
            collect_column_refs(pattern, cols);
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_column_refs(expr, cols);
            collect_column_refs(low, cols);
            collect_column_refs(high, cols);
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_column_refs(expr, cols);
            for e in list {
                collect_column_refs(e, cols);
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_column_refs(op, cols);
            }
            for cw in conditions {
                collect_column_refs(&cw.condition, cols);
                collect_column_refs(&cw.result, cols);
            }
            if let Some(e) = else_result {
                collect_column_refs(e, cols);
            }
        }
        SqlExpr::Cast { expr, .. } => {
            collect_column_refs(expr, cols);
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            collect_column_refs(expr, cols);
            collect_column_refs(pattern, cols);
        }
        SqlExpr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            collect_column_refs(expr, cols);
            if let Some(tw) = trim_what {
                collect_column_refs(tw, cols);
            }
            if let Some(chars) = trim_characters {
                for c in chars {
                    collect_column_refs(c, cols);
                }
            }
        }
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_column_refs(expr, cols);
            if let Some(f) = substring_from {
                collect_column_refs(f, cols);
            }
            if let Some(f) = substring_for {
                collect_column_refs(f, cols);
            }
        }
        SqlExpr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_column_refs(expr, cols);
            collect_column_refs(overlay_what, cols);
            collect_column_refs(overlay_from, cols);
            if let Some(f) = overlay_for {
                collect_column_refs(f, cols);
            }
        }
        SqlExpr::Extract { expr, .. } => {
            collect_column_refs(expr, cols);
        }
        SqlExpr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            collect_column_refs(timestamp, cols);
            collect_column_refs(time_zone, cols);
        }
        SqlExpr::Ceil { expr, .. } | SqlExpr::Floor { expr, .. } => {
            collect_column_refs(expr, cols);
        }
        SqlExpr::Position { expr, r#in } => {
            collect_column_refs(expr, cols);
            collect_column_refs(r#in, cols);
        }
        SqlExpr::InSubquery { expr, .. } => {
            collect_column_refs(expr, cols);
        }
        SqlExpr::InUnnest {
            expr, array_expr, ..
        } => {
            collect_column_refs(expr, cols);
            collect_column_refs(array_expr, cols);
        }
        SqlExpr::Convert { expr, .. } | SqlExpr::Collate { expr, .. } => {
            collect_column_refs(expr, cols);
        }
        // Literals, wildcards, etc. — no column refs.
        _ => {}
    }
}
