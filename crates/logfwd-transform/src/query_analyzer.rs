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
            walk_query(
                query,
                &mut referenced_columns,
                &mut uses_select_star,
                &mut except_fields,
                &mut where_clause,
            );
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
        if self.uses_select_star {
            ScanConfig {
                wanted_fields: vec![],
                extract_all: true,
                line_field_name: None,
                validate_utf8: false,
            }
        } else {
            use logfwd_core::scan_config::FieldSpec;
            use std::collections::HashSet;
            let mut seen = HashSet::new();
            let wanted: Vec<FieldSpec> = self
                .referenced_columns
                .iter()
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
                line_field_name: None,
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
                    .map(|c| strip_type_suffix(c))
                    .collect(),
            )
        };

        hints
    }
}

/// Recursively walk a Query wrapper (ORDER BY + body).
fn walk_query(
    query: &sqlast::Query,
    referenced_columns: &mut HashSet<String>,
    uses_select_star: &mut bool,
    except_fields: &mut Vec<String>,
    where_clause: &mut Option<SqlExpr>,
) {
    // Walk ORDER BY — columns may appear only here.
    if let Some(ref order_by) = query.order_by
        && let sqlast::OrderByKind::Expressions(exprs) = &order_by.kind
    {
        for ob in exprs {
            collect_column_refs(&ob.expr, referenced_columns);
        }
    }

    walk_set_expr(
        query.body.as_ref(),
        referenced_columns,
        uses_select_star,
        except_fields,
        where_clause,
    );
}

/// Recursively walk a `SetExpr`, collecting column refs from SELECT, WHERE,
/// GROUP BY, HAVING, FROM/JOIN, and WINDOW clauses. For `SetOperation`
/// (UNION/INTERSECT/EXCEPT) both branches are walked.
fn walk_set_expr(
    set_expr: &SetExpr,
    referenced_columns: &mut HashSet<String>,
    uses_select_star: &mut bool,
    except_fields: &mut Vec<String>,
    where_clause: &mut Option<SqlExpr>,
) {
    match set_expr {
        SetExpr::Select(select) => {
            for item in &select.projection {
                match item {
                    SelectItem::Wildcard(opts) => {
                        *uses_select_star = true;
                        extract_except_fields(opts, except_fields);
                    }
                    SelectItem::QualifiedWildcard(_, opts) => {
                        *uses_select_star = true;
                        extract_except_fields(opts, except_fields);
                    }
                    SelectItem::UnnamedExpr(expr) => {
                        collect_column_refs(expr, referenced_columns);
                    }
                    SelectItem::ExprWithAlias { expr, .. } => {
                        collect_column_refs(expr, referenced_columns);
                    }
                }
            }

            if let Some(ref selection) = select.selection {
                collect_column_refs(selection, referenced_columns);
                *where_clause = Some(selection.clone());
            }

            if let sqlast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                for e in exprs {
                    collect_column_refs(e, referenced_columns);
                }
            }

            if let Some(ref having) = select.having {
                collect_column_refs(having, referenced_columns);
            }

            for table_with_joins in &select.from {
                walk_table_with_joins(
                    table_with_joins,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
            }

            for sqlast::NamedWindowDefinition(_, named_expr) in &select.named_window {
                if let sqlast::NamedWindowExpr::WindowSpec(spec) = named_expr {
                    for e in &spec.partition_by {
                        collect_column_refs(e, referenced_columns);
                    }
                    for ob in &spec.order_by {
                        collect_column_refs(&ob.expr, referenced_columns);
                    }
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            // Set-operation branches can each have independent WHERE clauses.
            // We intentionally avoid extracting filter hints from compound
            // queries to prevent pushing one branch's predicate to all input.
            let mut left_where = None;
            walk_set_expr(
                left,
                referenced_columns,
                uses_select_star,
                except_fields,
                &mut left_where,
            );
            let mut right_where = None;
            walk_set_expr(
                right,
                referenced_columns,
                uses_select_star,
                except_fields,
                &mut right_where,
            );
            *where_clause = None;
        }
        SetExpr::Query(query) => {
            walk_query(
                query,
                referenced_columns,
                uses_select_star,
                except_fields,
                where_clause,
            );
        }
        // Values, Table, etc. — no column refs to extract.
        _ => {}
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

fn walk_table_factor(
    factor: &sqlast::TableFactor,
    referenced_columns: &mut HashSet<String>,
    uses_select_star: &mut bool,
    except_fields: &mut Vec<String>,
) {
    match factor {
        sqlast::TableFactor::Table { args, .. } => {
            if let Some(args) = args {
                collect_function_args(&args.args, referenced_columns);
            }
        }
        sqlast::TableFactor::Derived { subquery, .. } => {
            let mut nested_where = None;
            walk_query(
                subquery,
                referenced_columns,
                uses_select_star,
                except_fields,
                &mut nested_where,
            );
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_table_with_joins(
                table_with_joins,
                referenced_columns,
                uses_select_star,
                except_fields,
            );
        }
        sqlast::TableFactor::TableFunction { expr, .. } => {
            collect_column_refs(expr, referenced_columns);
        }
        sqlast::TableFactor::Function { args, .. } => {
            collect_function_args(args, referenced_columns);
        }
        sqlast::TableFactor::UNNEST { array_exprs, .. } => {
            for expr in array_exprs {
                collect_column_refs(expr, referenced_columns);
            }
        }
        sqlast::TableFactor::Pivot {
            table,
            aggregate_functions,
            value_column,
            value_source,
            default_on_null,
            ..
        } => {
            walk_table_factor(table, referenced_columns, uses_select_star, except_fields);
            for agg in aggregate_functions {
                collect_column_refs(&agg.expr, referenced_columns);
            }
            for col in value_column {
                referenced_columns.insert(col.value.clone());
            }
            match value_source {
                sqlast::PivotValueSource::List(values) => {
                    for value in values {
                        collect_column_refs(&value.expr, referenced_columns);
                    }
                }
                sqlast::PivotValueSource::Any(order_by) => {
                    for ob in order_by {
                        collect_column_refs(&ob.expr, referenced_columns);
                    }
                }
                sqlast::PivotValueSource::Subquery(query) => {
                    let mut nested_where = None;
                    walk_query(
                        query,
                        referenced_columns,
                        uses_select_star,
                        except_fields,
                        &mut nested_where,
                    );
                }
            }
            if let Some(expr) = default_on_null {
                collect_column_refs(expr, referenced_columns);
            }
        }
        sqlast::TableFactor::Unpivot { table, columns, .. } => {
            walk_table_factor(table, referenced_columns, uses_select_star, except_fields);
            for col in columns {
                referenced_columns.insert(col.value.clone());
            }
        }
        sqlast::TableFactor::JsonTable { json_expr, .. }
        | sqlast::TableFactor::OpenJsonTable { json_expr, .. } => {
            collect_column_refs(json_expr, referenced_columns);
        }
        sqlast::TableFactor::MatchRecognize {
            table,
            partition_by,
            order_by,
            measures,
            rows_per_match: _,
            after_match_skip: _,
            pattern,
            symbols,
            alias: _,
        } => {
            walk_table_factor(table, referenced_columns, uses_select_star, except_fields);
            for expr in partition_by {
                collect_column_refs(expr, referenced_columns);
            }
            for order_expr in order_by {
                collect_column_refs(&order_expr.expr, referenced_columns);
            }
            for measure in measures {
                collect_column_refs(&measure.expr, referenced_columns);
            }
            for symbol_def in symbols {
                collect_column_refs(&symbol_def.definition, referenced_columns);
            }
            walk_match_recognize_pattern(pattern);
        }
    }
}

fn walk_table_with_joins(
    table_with_joins: &sqlast::TableWithJoins,
    referenced_columns: &mut HashSet<String>,
    uses_select_star: &mut bool,
    except_fields: &mut Vec<String>,
) {
    walk_table_factor(
        &table_with_joins.relation,
        referenced_columns,
        uses_select_star,
        except_fields,
    );
    for join in &table_with_joins.joins {
        walk_table_factor(
            &join.relation,
            referenced_columns,
            uses_select_star,
            except_fields,
        );
        if let sqlast::JoinOperator::AsOf {
            match_condition, ..
        } = &join.join_operator
        {
            collect_column_refs(match_condition, referenced_columns);
        }

        if let Some(constraint) = extract_join_constraint(&join.join_operator) {
            collect_join_constraint_columns(constraint, referenced_columns);
        }
    }
}

/// Walk a MATCH_RECOGNIZE pattern tree.
///
/// Patterns name symbols rather than columns, so this traversal is currently
/// just structural recursion that keeps the analyzer complete if the AST grows.
fn walk_match_recognize_pattern(pattern: &sqlast::MatchRecognizePattern) {
    match pattern {
        sqlast::MatchRecognizePattern::Symbol(_)
        | sqlast::MatchRecognizePattern::Exclude(_)
        | sqlast::MatchRecognizePattern::Permute(_) => {}
        sqlast::MatchRecognizePattern::Concat(patterns)
        | sqlast::MatchRecognizePattern::Alternation(patterns) => {
            for nested in patterns {
                walk_match_recognize_pattern(nested);
            }
        }
        sqlast::MatchRecognizePattern::Group(pattern)
        | sqlast::MatchRecognizePattern::Repetition(pattern, _) => {
            walk_match_recognize_pattern(pattern);
        }
    }
}

/// Extract the `JoinConstraint` from any `JoinOperator` variant that carries one.
fn extract_join_constraint(op: &sqlast::JoinOperator) -> Option<&sqlast::JoinConstraint> {
    use sqlast::JoinOperator as J;
    match op {
        J::Join(c)
        | J::Inner(c)
        | J::Left(c)
        | J::LeftOuter(c)
        | J::Right(c)
        | J::RightOuter(c)
        | J::FullOuter(c)
        | J::Semi(c)
        | J::LeftSemi(c)
        | J::RightSemi(c)
        | J::Anti(c)
        | J::LeftAnti(c)
        | J::RightAnti(c)
        | J::AsOf { constraint: c, .. } => Some(c),
        J::CrossJoin | J::CrossApply | J::OuterApply => None,
    }
}

/// Collect column references from a `JoinConstraint`.
fn collect_join_constraint_columns(
    constraint: &sqlast::JoinConstraint,
    cols: &mut HashSet<String>,
) {
    match constraint {
        sqlast::JoinConstraint::On(expr) => {
            collect_column_refs(expr, cols);
        }
        sqlast::JoinConstraint::Using(using_cols) => {
            for obj_name in using_cols {
                if let Some(part) = obj_name.0.last()
                    && let Some(ident) = part.as_ident()
                {
                    cols.insert(ident.value.clone());
                }
            }
        }
        sqlast::JoinConstraint::Natural | sqlast::JoinConstraint::None => {}
    }
}

fn collect_function_arg_refs(arg: &sqlast::FunctionArg, cols: &mut HashSet<String>) {
    match arg {
        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e))
        | sqlast::FunctionArg::Named {
            arg: sqlast::FunctionArgExpr::Expr(e),
            ..
        } => {
            collect_column_refs(e, cols);
        }
        sqlast::FunctionArg::ExprNamed { name, arg, .. } => {
            collect_column_refs(name, cols);
            if let sqlast::FunctionArgExpr::Expr(e) = arg {
                collect_column_refs(e, cols);
            }
        }
        _ => {}
    }
}

fn collect_function_args(args: &[sqlast::FunctionArg], cols: &mut HashSet<String>) {
    for arg in args {
        collect_function_arg_refs(arg, cols);
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
                        collect_function_arg_refs(arg, cols);
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
            expr, trim_what, ..
        } => {
            collect_column_refs(expr, cols);
            if let Some(tw) = trim_what {
                collect_column_refs(tw, cols);
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
