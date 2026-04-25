//! SQL query analysis and predicate-hint extraction.

use std::collections::HashSet;

use datafusion::sql::sqlparser::ast::{
    self as sqlast, Expr as SqlExpr, SelectItem, SetExpr, Statement, WildcardAdditionalOptions,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use ffwd_core::scan_config::ScanConfig;
use ffwd_types::field_names;
use ffwd_types::source_metadata::{SourceMetadataPlan, SourcePathColumn};

use crate::TransformError;

#[derive(Default)]
struct SourceMetadataUsage {
    referenced_columns: HashSet<String>,
}

#[derive(Default)]
struct SourceMetadataScope {
    logs_aliases: HashSet<String>,
    outer_logs_aliases: HashSet<String>,
    cte_names: HashSet<String>,
    logs_backed_cte_names: HashSet<String>,
    visible_relations: usize,
    logs_relations: usize,
}

impl SourceMetadataScope {
    fn with_outer(outer_scope: &Self) -> Self {
        let mut outer_logs_aliases = outer_scope.outer_logs_aliases.clone();
        outer_logs_aliases.extend(outer_scope.logs_aliases.iter().cloned());
        Self {
            outer_logs_aliases,
            ..Self::default()
        }
    }

    fn matches_logs_qualifier(&self, qualifier: &str) -> bool {
        self.logs_aliases
            .iter()
            .chain(self.outer_logs_aliases.iter())
            .any(|alias| alias.eq_ignore_ascii_case(qualifier))
    }

    fn bare_identifiers_resolve_to_logs(&self) -> bool {
        self.logs_relations == 1 && self.visible_relations == 1
    }
}

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
    /// Source metadata references scoped to the `logs` relation.
    source_metadata_usage: SourceMetadataUsage,
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
        let mut source_metadata_usage = SourceMetadataUsage::default();
        let mut where_clause = None;

        if let Statement::Query(query) = stmt {
            walk_query(
                query,
                &mut referenced_columns,
                &mut uses_select_star,
                &mut except_fields,
                &mut where_clause,
            );
            collect_logs_source_metadata_from_query(query, &mut source_metadata_usage);
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
            source_metadata_usage,
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
        let row_predicate = self.extract_scan_predicate();

        if self.uses_select_star {
            ScanConfig {
                wanted_fields: vec![],
                extract_all: true,
                line_field_name: None,
                validate_utf8: false,
                row_predicate,
            }
        } else {
            use ffwd_core::scan_config::FieldSpec;
            use std::collections::HashSet;
            let mut seen = HashSet::new();
            let wanted: Vec<FieldSpec> = self
                .referenced_columns
                .iter()
                .map(|name| strip_type_suffix(name))
                .map(|name| name.to_ascii_lowercase())
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
                row_predicate,
            }
        }
    }

    /// Extract a `ScanPredicate` from the WHERE clause, if present.
    ///
    /// Only simple predicates are extracted (equality, comparison, IS NULL,
    /// AND chains). Complex expressions (OR at top level, functions, LIKE,
    /// subqueries) are left to DataFusion as residual filters.
    fn extract_scan_predicate(&self) -> Option<ScanPredicate> {
        let where_expr = self.where_clause.as_ref()?;
        extract_scan_predicate_from_expr(where_expr)
    }

    /// Source metadata columns that must be attached post-scan before SQL.
    ///
    /// Explicit references need their columns for projection/filter/join
    /// evaluation. Wildcard projections remain narrow and never attach source
    /// metadata by themselves.
    pub fn source_metadata_plan(&self) -> SourceMetadataPlan {
        self.explicit_source_metadata_plan()
    }

    /// Source metadata columns explicitly referenced by SQL expressions.
    ///
    /// This excludes wildcard projection and wildcard EXCEPT lists so callers
    /// can reject source metadata dependencies only when SQL actually consumes
    /// those metadata values.
    pub fn explicit_source_metadata_plan(&self) -> SourceMetadataPlan {
        source_metadata_plan_for_names(&self.source_metadata_usage.referenced_columns)
    }

    /// Whether the input layer must expose a source path column for this query.
    pub fn source_path_required(&self) -> bool {
        self.source_metadata_plan().has_source_path()
    }

    /// Extract filter hints from the SQL for predicate pushdown.
    ///
    /// Walks the WHERE clause looking for simple predicates on known syslog
    /// columns (severity, facility) that can be pushed to input sources.
    /// Only predicates in top-level AND chains are extracted — OR'd predicates
    /// are left for DataFusion since pushing them could miss matching rows.
    pub fn filter_hints(&self) -> ffwd_types::filter_hints::FilterHints {
        let mut hints = ffwd_types::filter_hints::FilterHints::default();

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
    // Walk CTE (WITH clause) bodies — columns inside CTEs are referenced.
    if let Some(ref with) = query.with {
        for cte in &with.cte_tables {
            let mut cte_where = None;
            walk_query(
                &cte.query,
                referenced_columns,
                uses_select_star,
                except_fields,
                &mut cte_where,
            );
        }
    }

    // Walk ORDER BY — columns may appear only here.
    if let Some(ref order_by) = query.order_by
        && let sqlast::OrderByKind::Expressions(exprs) = &order_by.kind
    {
        for ob in exprs {
            collect_column_refs_with_wildcards(
                &ob.expr,
                referenced_columns,
                uses_select_star,
                except_fields,
            );
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
                        collect_column_refs_with_wildcards(
                            expr,
                            referenced_columns,
                            uses_select_star,
                            except_fields,
                        );
                    }
                    SelectItem::ExprWithAlias { expr, .. } => {
                        collect_column_refs_with_wildcards(
                            expr,
                            referenced_columns,
                            uses_select_star,
                            except_fields,
                        );
                    }
                }
            }

            if let Some(ref selection) = select.selection {
                collect_column_refs_with_wildcards(
                    selection,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
                *where_clause = Some(selection.clone());
            }

            if let sqlast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                for e in exprs {
                    collect_column_refs_with_wildcards(
                        e,
                        referenced_columns,
                        uses_select_star,
                        except_fields,
                    );
                }
            }

            if let Some(ref having) = select.having {
                collect_column_refs_with_wildcards(
                    having,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
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
                        collect_column_refs_with_wildcards(
                            e,
                            referenced_columns,
                            uses_select_star,
                            except_fields,
                        );
                    }
                    for ob in &spec.order_by {
                        collect_column_refs_with_wildcards(
                            &ob.expr,
                            referenced_columns,
                            uses_select_star,
                            except_fields,
                        );
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
fn extract_pushable_predicates(expr: &SqlExpr, hints: &mut ffwd_types::filter_hints::FilterHints) {
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
///
/// For general column-reference collection, qualified identifiers like
/// `logs.level` are accepted (the last part is the column name).
fn expr_as_column(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => Some(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

/// Extract column name from a bare (unqualified) identifier only.
///
/// Used by scan-predicate extraction to avoid pushing predicates for
/// qualified columns (e.g. `env.level`) from joins. Conservative: won't
/// push `logs.level = 'error'` either, but the residual SQL filter handles
/// it correctly.
fn expr_as_bare_column(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => {
            let name = ident.value.clone();
            if is_post_scan_metadata_column(&name) {
                None
            } else {
                Some(name)
            }
        }
        // Qualified identifiers (table.column) are not pushed — they may
        // reference a joined relation rather than the scanned `logs` table.
        SqlExpr::CompoundIdentifier(_) => None,
        _ => None,
    }
}

fn is_post_scan_metadata_column(name: &str) -> bool {
    name.eq_ignore_ascii_case(field_names::ECS_FILE_PATH)
        || name.eq_ignore_ascii_case(field_names::OTEL_LOG_FILE_PATH)
        || name.eq_ignore_ascii_case(field_names::VECTOR_FILE)
        || name.eq_ignore_ascii_case(field_names::SOURCE_ID)
        || name.eq_ignore_ascii_case(field_names::TIMESTAMP_AT)
        || name.eq_ignore_ascii_case(field_names::TIMESTAMP_UNDERSCORE)
        || name.eq_ignore_ascii_case(field_names::CRI_STREAM)
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

// ---------------------------------------------------------------------------
// ScanPredicate extraction from SQL WHERE AST
// ---------------------------------------------------------------------------

use ffwd_core::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

/// Convert a SQL WHERE expression into a `ScanPredicate`.
///
/// Returns `None` for expressions that cannot be pushed down (OR at top
/// level, function calls, LIKE, subqueries, etc.). AND chains are
/// recursively decomposed — only the pushable conjuncts are included.
fn extract_scan_predicate_from_expr(expr: &SqlExpr) -> Option<ScanPredicate> {
    match expr {
        // AND: recurse into both sides, collect pushable conjuncts.
        SqlExpr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::And,
            right,
        } => {
            let mut conjuncts = Vec::new();
            if let Some(p) = extract_scan_predicate_from_expr(left) {
                conjuncts.push(p);
            }
            if let Some(p) = extract_scan_predicate_from_expr(right) {
                conjuncts.push(p);
            }
            match conjuncts.len() {
                0 => None,
                1 => Some(conjuncts.remove(0)),
                _ => Some(ScanPredicate::And(conjuncts)),
            }
        }

        // OR: only push if all branches are Eq on the same column (→ InList).
        SqlExpr::BinaryOp {
            left,
            op: sqlast::BinaryOperator::Or,
            right,
        } => {
            let left_pred = extract_scan_predicate_from_expr(left)?;
            let right_pred = extract_scan_predicate_from_expr(right)?;
            try_merge_or_to_in_list(left_pred, right_pred)
        }

        // column op literal
        SqlExpr::BinaryOp { left, op, right } => {
            // Try column on left, literal on right.
            if let (Some(col), Some(val)) = (expr_as_bare_column(left), expr_as_scalar_value(right))
                && let Some(cmp_op) = sql_op_to_cmp_op(op)
            {
                return Some(ScanPredicate::Compare {
                    field: col.to_ascii_lowercase(),
                    op: cmp_op,
                    value: val,
                });
            }
            // Try literal on left, column on right (reversed operand order).
            if let (Some(val), Some(col)) = (expr_as_scalar_value(left), expr_as_bare_column(right))
                && let Some(cmp_op) = sql_op_to_cmp_op_reversed(op)
            {
                return Some(ScanPredicate::Compare {
                    field: col.to_ascii_lowercase(),
                    op: cmp_op,
                    value: val,
                });
            }
            None
        }

        // IS NULL / IS NOT NULL
        SqlExpr::IsNull(inner) => {
            let col = expr_as_bare_column(inner)?;
            Some(ScanPredicate::IsNull {
                field: col.to_ascii_lowercase(),
                negated: false,
            })
        }
        SqlExpr::IsNotNull(inner) => {
            let col = expr_as_bare_column(inner)?;
            Some(ScanPredicate::IsNull {
                field: col.to_ascii_lowercase(),
                negated: true,
            })
        }

        // IN list: column IN (v1, v2, ...)
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let col = expr_as_bare_column(expr)?;
            let values: Vec<ScalarValue> = list.iter().filter_map(expr_as_scalar_value).collect();
            // Only push if all list elements are literal scalars.
            if values.len() != list.len() || values.is_empty() {
                return None;
            }
            Some(ScanPredicate::InList {
                field: col.to_ascii_lowercase(),
                values,
                negated: *negated,
            })
        }

        // LIKE: only push prefix ('foo%') and contains ('%foo%') patterns.
        // Custom ESCAPE clauses change wildcard interpretation — don't push.
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            escape_char,
            ..
        } if !negated && escape_char.is_none() => {
            let col = expr_as_bare_column(expr)?;
            let pat = expr_as_string_literal(pattern)?;
            if let Some(prefix) = pat.strip_suffix('%')
                && !prefix.contains('%')
                && !prefix.contains('_')
            {
                // 'prefix%' → StartsWith
                return Some(ScanPredicate::StartsWith {
                    field: col.to_ascii_lowercase(),
                    prefix: prefix.to_string(),
                });
            }
            if let Some(inner) = pat.strip_prefix('%').and_then(|s| s.strip_suffix('%'))
                && !inner.contains('%')
                && !inner.contains('_')
            {
                // '%substring%' → Contains
                return Some(ScanPredicate::Contains {
                    field: col.to_ascii_lowercase(),
                    substring: inner.to_string(),
                });
            }
            None
        }

        // Parenthesized expression
        SqlExpr::Nested(inner) => extract_scan_predicate_from_expr(inner),

        // Anything else — not pushable.
        _ => None,
    }
}

/// Try to merge two OR'd predicates into an InList when both are
/// Eq comparisons on the same field.
fn try_merge_or_to_in_list(left: ScanPredicate, right: ScanPredicate) -> Option<ScanPredicate> {
    // Flatten: collect all Eq values for a single field from both sides.
    let mut field_name = None;
    let mut values = Vec::new();

    fn collect_eq_values(
        pred: &ScanPredicate,
        field_name: &mut Option<String>,
        values: &mut Vec<ScalarValue>,
    ) -> bool {
        match pred {
            ScanPredicate::Compare {
                field, op, value, ..
            } if *op == CmpOp::Eq => {
                if let Some(existing) = field_name.as_ref() {
                    if !existing.eq_ignore_ascii_case(field) {
                        return false; // Different field — can't merge.
                    }
                } else {
                    *field_name = Some(field.clone());
                }
                values.push(value.clone());
                true
            }
            ScanPredicate::InList {
                field: f,
                values: vs,
                negated: false,
            } => {
                if let Some(existing) = field_name.as_ref() {
                    if !existing.eq_ignore_ascii_case(f) {
                        return false;
                    }
                } else {
                    *field_name = Some(f.clone());
                }
                values.extend(vs.iter().cloned());
                true
            }
            ScanPredicate::Or(preds) => preds
                .iter()
                .all(|p| collect_eq_values(p, field_name, values)),
            _ => false,
        }
    }

    if !collect_eq_values(&left, &mut field_name, &mut values) {
        return None;
    }
    if !collect_eq_values(&right, &mut field_name, &mut values) {
        return None;
    }

    Some(ScanPredicate::InList {
        field: field_name?,
        values,
        negated: false,
    })
}

/// Convert a SQL binary operator to a `CmpOp`.
fn sql_op_to_cmp_op(op: &sqlast::BinaryOperator) -> Option<CmpOp> {
    match op {
        sqlast::BinaryOperator::Eq => Some(CmpOp::Eq),
        sqlast::BinaryOperator::NotEq => Some(CmpOp::Ne),
        sqlast::BinaryOperator::Lt => Some(CmpOp::Lt),
        sqlast::BinaryOperator::LtEq => Some(CmpOp::Le),
        sqlast::BinaryOperator::Gt => Some(CmpOp::Gt),
        sqlast::BinaryOperator::GtEq => Some(CmpOp::Ge),
        _ => None,
    }
}

/// Convert a SQL binary operator to a `CmpOp` for reversed operand order.
/// `literal < column` → column > literal, etc.
fn sql_op_to_cmp_op_reversed(op: &sqlast::BinaryOperator) -> Option<CmpOp> {
    match op {
        sqlast::BinaryOperator::Eq => Some(CmpOp::Eq),
        sqlast::BinaryOperator::NotEq => Some(CmpOp::Ne),
        sqlast::BinaryOperator::Lt => Some(CmpOp::Gt),
        sqlast::BinaryOperator::LtEq => Some(CmpOp::Ge),
        sqlast::BinaryOperator::Gt => Some(CmpOp::Lt),
        sqlast::BinaryOperator::GtEq => Some(CmpOp::Le),
        _ => None,
    }
}

/// Extract a `ScalarValue` from a SQL literal expression.
fn expr_as_scalar_value(expr: &SqlExpr) -> Option<ScalarValue> {
    match expr {
        SqlExpr::Value(v) => match &v.value {
            sqlast::Value::SingleQuotedString(s) => Some(ScalarValue::Str(s.clone())),
            sqlast::Value::Number(s, _) => {
                // Try integer first, then float.
                if let Ok(n) = s.parse::<i64>() {
                    Some(ScalarValue::Int(n))
                } else if let Ok(f) = s.parse::<f64>() {
                    Some(ScalarValue::Float(f))
                } else {
                    None
                }
            }
            sqlast::Value::Boolean(b) => Some(ScalarValue::Bool(*b)),
            sqlast::Value::Null => Some(ScalarValue::Null),
            _ => None,
        },
        // Handle negative numbers: UnaryOp { Minus, Number }
        SqlExpr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            SqlExpr::Value(v) => match &v.value {
                sqlast::Value::Number(s, _) => {
                    if let Ok(n) = s.parse::<i64>() {
                        Some(ScalarValue::Int(-n))
                    } else if let Ok(f) = s.parse::<f64>() {
                        Some(ScalarValue::Float(-f))
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Extract a plain string literal from a SQL expression.
fn expr_as_string_literal(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Value(v) => match &v.value {
            sqlast::Value::SingleQuotedString(s) => Some(s.clone()),
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
                collect_function_args_with_wildcards(
                    &args.args,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
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
            collect_column_refs_with_wildcards(
                expr,
                referenced_columns,
                uses_select_star,
                except_fields,
            );
        }
        sqlast::TableFactor::Function { args, .. } => {
            collect_function_args_with_wildcards(
                args,
                referenced_columns,
                uses_select_star,
                except_fields,
            );
        }
        sqlast::TableFactor::UNNEST { array_exprs, .. } => {
            for expr in array_exprs {
                collect_column_refs_with_wildcards(
                    expr,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
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
                collect_column_refs_with_wildcards(
                    &agg.expr,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
            }
            for col in value_column {
                collect_column_refs_with_wildcards(
                    col,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
            }
            match value_source {
                sqlast::PivotValueSource::List(values) => {
                    for value in values {
                        collect_column_refs_with_wildcards(
                            &value.expr,
                            referenced_columns,
                            uses_select_star,
                            except_fields,
                        );
                    }
                }
                sqlast::PivotValueSource::Any(order_by) => {
                    for ob in order_by {
                        collect_column_refs_with_wildcards(
                            &ob.expr,
                            referenced_columns,
                            uses_select_star,
                            except_fields,
                        );
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
                collect_column_refs_with_wildcards(
                    expr,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
            }
        }
        sqlast::TableFactor::Unpivot { table, columns, .. } => {
            walk_table_factor(table, referenced_columns, uses_select_star, except_fields);
            for col in columns {
                collect_column_refs_with_wildcards(
                    &col.expr,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
            }
        }
        sqlast::TableFactor::JsonTable { json_expr, .. }
        | sqlast::TableFactor::OpenJsonTable { json_expr, .. } => {
            collect_column_refs_with_wildcards(
                json_expr,
                referenced_columns,
                uses_select_star,
                except_fields,
            );
        }
        sqlast::TableFactor::MatchRecognize {
            table,
            partition_by,
            order_by,
            measures,
            pattern,
            symbols,
            ..
        } => {
            walk_table_factor(table, referenced_columns, uses_select_star, except_fields);
            for expr in partition_by {
                collect_column_refs_with_wildcards(
                    expr,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
            }
            for order_expr in order_by {
                collect_column_refs_with_wildcards(
                    &order_expr.expr,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
            }
            for measure in measures {
                collect_column_refs_with_wildcards(
                    &measure.expr,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
            }
            for symbol_def in symbols {
                collect_column_refs_with_wildcards(
                    &symbol_def.definition,
                    referenced_columns,
                    uses_select_star,
                    except_fields,
                );
            }
            walk_match_recognize_pattern(pattern);
        }
        // New sqlparser variants that don't require column reference extraction.
        sqlast::TableFactor::XmlTable { .. } | sqlast::TableFactor::SemanticView { .. } => {}
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
            collect_column_refs_with_wildcards(
                match_condition,
                referenced_columns,
                uses_select_star,
                except_fields,
            );
        }

        if let Some(constraint) = extract_join_constraint(&join.join_operator) {
            collect_join_constraint_columns(
                constraint,
                referenced_columns,
                uses_select_star,
                except_fields,
            );
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
        J::StraightJoin(c) => Some(c),
        J::CrossJoin(_) | J::CrossApply | J::OuterApply => None,
    }
}

/// Collect column references from a `JoinConstraint`.
fn collect_join_constraint_columns(
    constraint: &sqlast::JoinConstraint,
    cols: &mut HashSet<String>,
    uses_select_star: &mut bool,
    except_fields: &mut Vec<String>,
) {
    match constraint {
        sqlast::JoinConstraint::On(expr) => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
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

fn collect_function_arg_refs_with_wildcards(
    arg: &sqlast::FunctionArg,
    cols: &mut HashSet<String>,
    uses_select_star: &mut bool,
    except_fields: &mut Vec<String>,
) {
    match arg {
        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e))
        | sqlast::FunctionArg::Named {
            arg: sqlast::FunctionArgExpr::Expr(e),
            ..
        } => {
            collect_column_refs_with_wildcards(e, cols, uses_select_star, except_fields);
        }
        sqlast::FunctionArg::ExprNamed { name, arg, .. } => {
            collect_column_refs_with_wildcards(name, cols, uses_select_star, except_fields);
            if let sqlast::FunctionArgExpr::Expr(e) = arg {
                collect_column_refs_with_wildcards(e, cols, uses_select_star, except_fields);
            }
        }
        _ => {}
    }
}

fn collect_function_args_with_wildcards(
    args: &[sqlast::FunctionArg],
    cols: &mut HashSet<String>,
    uses_select_star: &mut bool,
    except_fields: &mut Vec<String>,
) {
    for arg in args {
        collect_function_arg_refs_with_wildcards(arg, cols, uses_select_star, except_fields);
    }
}

fn collect_column_refs_with_wildcards(
    expr: &SqlExpr,
    cols: &mut HashSet<String>,
    uses_select_star: &mut bool,
    except_fields: &mut Vec<String>,
) {
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
            collect_column_refs_with_wildcards(left, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(right, cols, uses_select_star, except_fields);
        }
        SqlExpr::UnaryOp { expr, .. } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
        }
        SqlExpr::Function(func) => {
            match &func.args {
                sqlast::FunctionArguments::List(arg_list) => {
                    for arg in &arg_list.args {
                        collect_function_arg_refs_with_wildcards(
                            arg,
                            cols,
                            uses_select_star,
                            except_fields,
                        );
                    }
                }
                sqlast::FunctionArguments::None => {}
                sqlast::FunctionArguments::Subquery(subquery) => {
                    collect_columns_from_subquery(subquery, cols, uses_select_star, except_fields);
                }
            }
            // Walk OVER clause: columns in PARTITION BY and ORDER BY are
            // only referenced in func.over, not in func.args.
            if let Some(sqlast::WindowType::WindowSpec(spec)) = &func.over {
                for e in &spec.partition_by {
                    collect_column_refs_with_wildcards(e, cols, uses_select_star, except_fields);
                }
                for ob in &spec.order_by {
                    collect_column_refs_with_wildcards(
                        &ob.expr,
                        cols,
                        uses_select_star,
                        except_fields,
                    );
                }
            }
        }
        SqlExpr::Nested(inner) => {
            collect_column_refs_with_wildcards(inner, cols, uses_select_star, except_fields);
        }
        SqlExpr::IsNull(e)
        | SqlExpr::IsNotNull(e)
        | SqlExpr::IsTrue(e)
        | SqlExpr::IsNotTrue(e)
        | SqlExpr::IsFalse(e)
        | SqlExpr::IsNotFalse(e)
        | SqlExpr::IsUnknown(e)
        | SqlExpr::IsNotUnknown(e) => {
            collect_column_refs_with_wildcards(e, cols, uses_select_star, except_fields);
        }
        SqlExpr::IsDistinctFrom(left, right) | SqlExpr::IsNotDistinctFrom(left, right) => {
            collect_column_refs_with_wildcards(left, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(right, cols, uses_select_star, except_fields);
        }
        SqlExpr::SimilarTo { expr, pattern, .. } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(pattern, cols, uses_select_star, except_fields);
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(low, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(high, cols, uses_select_star, except_fields);
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            for e in list {
                collect_column_refs_with_wildcards(e, cols, uses_select_star, except_fields);
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_column_refs_with_wildcards(op, cols, uses_select_star, except_fields);
            }
            for cw in conditions {
                collect_column_refs_with_wildcards(
                    &cw.condition,
                    cols,
                    uses_select_star,
                    except_fields,
                );
                collect_column_refs_with_wildcards(
                    &cw.result,
                    cols,
                    uses_select_star,
                    except_fields,
                );
            }
            if let Some(e) = else_result {
                collect_column_refs_with_wildcards(e, cols, uses_select_star, except_fields);
            }
        }
        SqlExpr::Cast { expr, .. } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
        }
        SqlExpr::Like { expr, pattern, .. } | SqlExpr::ILike { expr, pattern, .. } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(pattern, cols, uses_select_star, except_fields);
        }
        SqlExpr::Trim {
            expr, trim_what, ..
        } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            if let Some(tw) = trim_what {
                collect_column_refs_with_wildcards(tw, cols, uses_select_star, except_fields);
            }
        }
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            if let Some(f) = substring_from {
                collect_column_refs_with_wildcards(f, cols, uses_select_star, except_fields);
            }
            if let Some(f) = substring_for {
                collect_column_refs_with_wildcards(f, cols, uses_select_star, except_fields);
            }
        }
        SqlExpr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(overlay_what, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(overlay_from, cols, uses_select_star, except_fields);
            if let Some(f) = overlay_for {
                collect_column_refs_with_wildcards(f, cols, uses_select_star, except_fields);
            }
        }
        SqlExpr::Extract { expr, .. } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
        }
        SqlExpr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            collect_column_refs_with_wildcards(timestamp, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(time_zone, cols, uses_select_star, except_fields);
        }
        SqlExpr::Ceil { expr, .. } | SqlExpr::Floor { expr, .. } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
        }
        SqlExpr::Position { expr, r#in } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(r#in, cols, uses_select_star, except_fields);
        }
        SqlExpr::InSubquery { expr, subquery, .. } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            collect_columns_from_subquery(subquery, cols, uses_select_star, except_fields);
        }
        SqlExpr::InUnnest {
            expr, array_expr, ..
        } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
            collect_column_refs_with_wildcards(array_expr, cols, uses_select_star, except_fields);
        }
        SqlExpr::Convert { expr, .. } | SqlExpr::Collate { expr, .. } => {
            collect_column_refs_with_wildcards(expr, cols, uses_select_star, except_fields);
        }
        SqlExpr::Exists { subquery, .. } | SqlExpr::Subquery(subquery) => {
            collect_columns_from_subquery(subquery, cols, uses_select_star, except_fields);
        }
        // Literals, wildcards, etc. — no column refs.
        _ => {}
    }
}

/// Walk a subquery and collect column references from it.
/// Used by InSubquery, Exists, Subquery (scalar), and FunctionArguments::Subquery.
fn collect_columns_from_subquery(
    query: &sqlast::Query,
    cols: &mut HashSet<String>,
    uses_select_star: &mut bool,
    except_fields: &mut Vec<String>,
) {
    let mut nested_where = None;
    walk_query(
        query,
        cols,
        uses_select_star,
        except_fields,
        &mut nested_where,
    );
}

fn collect_logs_source_metadata_from_query(query: &sqlast::Query, usage: &mut SourceMetadataUsage) {
    collect_logs_source_metadata_from_query_scoped(
        query,
        usage,
        &HashSet::new(),
        &HashSet::new(),
        &SourceMetadataScope::default(),
    );
}

fn collect_logs_source_metadata_from_query_scoped(
    query: &sqlast::Query,
    usage: &mut SourceMetadataUsage,
    inherited_cte_names: &HashSet<String>,
    inherited_logs_backed_cte_names: &HashSet<String>,
    outer_scope: &SourceMetadataScope,
) {
    let cte_names = cte_names_for_query(query);
    let mut accumulated_cte_names = inherited_cte_names.clone();
    let mut accumulated_logs_backed_cte_names = inherited_logs_backed_cte_names.clone();
    if let Some(ref with) = query.with {
        for cte in &with.cte_tables {
            collect_logs_source_metadata_from_query_scoped(
                &cte.query,
                usage,
                &accumulated_cte_names,
                &accumulated_logs_backed_cte_names,
                outer_scope,
            );
            if query_exposes_logs_relation(
                &cte.query,
                &accumulated_cte_names,
                &accumulated_logs_backed_cte_names,
            ) {
                accumulated_logs_backed_cte_names.insert(cte.alias.name.value.clone());
            }
            accumulated_cte_names.insert(cte.alias.name.value.clone());
        }
    }
    let active_cte_names = combined_cte_names(inherited_cte_names, &cte_names);
    let active_logs_backed_cte_names = accumulated_logs_backed_cte_names;

    collect_logs_source_metadata_from_set_expr(
        query.body.as_ref(),
        usage,
        &active_cte_names,
        &active_logs_backed_cte_names,
        outer_scope,
    );

    if let Some(ref order_by) = query.order_by
        && let sqlast::OrderByKind::Expressions(exprs) = &order_by.kind
    {
        let scope = source_metadata_scope_for_set_expr(
            query.body.as_ref(),
            usage,
            &active_cte_names,
            &active_logs_backed_cte_names,
            outer_scope,
        );
        for ob in exprs {
            collect_logs_source_metadata_from_expr(&ob.expr, &scope, usage);
        }
    }
}

fn collect_logs_source_metadata_from_set_expr(
    set_expr: &SetExpr,
    usage: &mut SourceMetadataUsage,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
    outer_scope: &SourceMetadataScope,
) {
    match set_expr {
        SetExpr::Select(select) => {
            collect_logs_source_metadata_from_select(
                select,
                usage,
                cte_names,
                logs_backed_cte_names,
                outer_scope,
            );
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_logs_source_metadata_from_set_expr(
                left,
                usage,
                cte_names,
                logs_backed_cte_names,
                outer_scope,
            );
            collect_logs_source_metadata_from_set_expr(
                right,
                usage,
                cte_names,
                logs_backed_cte_names,
                outer_scope,
            );
        }
        SetExpr::Query(query) => {
            collect_logs_source_metadata_from_query_scoped(
                query,
                usage,
                cte_names,
                logs_backed_cte_names,
                outer_scope,
            );
        }
        _ => {}
    }
}

fn source_metadata_scope_for_set_expr(
    set_expr: &SetExpr,
    usage: &mut SourceMetadataUsage,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
    outer_scope: &SourceMetadataScope,
) -> SourceMetadataScope {
    match set_expr {
        SetExpr::Select(select) => source_metadata_scope_for_select(
            select,
            usage,
            cte_names,
            logs_backed_cte_names,
            outer_scope,
        ),
        SetExpr::Query(query) => {
            collect_logs_source_metadata_from_query_scoped(
                query,
                usage,
                cte_names,
                logs_backed_cte_names,
                outer_scope,
            );
            SourceMetadataScope::default()
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_logs_source_metadata_from_set_expr(
                left,
                usage,
                cte_names,
                logs_backed_cte_names,
                outer_scope,
            );
            collect_logs_source_metadata_from_set_expr(
                right,
                usage,
                cte_names,
                logs_backed_cte_names,
                outer_scope,
            );
            SourceMetadataScope::default()
        }
        _ => SourceMetadataScope::default(),
    }
}

fn collect_logs_source_metadata_from_select(
    select: &sqlast::Select,
    usage: &mut SourceMetadataUsage,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
    outer_scope: &SourceMetadataScope,
) {
    let scope = source_metadata_scope_for_select(
        select,
        usage,
        cte_names,
        logs_backed_cte_names,
        outer_scope,
    );

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {}
            SelectItem::QualifiedWildcard(kind, _) => match kind {
                sqlast::SelectItemQualifiedWildcardKind::ObjectName(_) => {}
                sqlast::SelectItemQualifiedWildcardKind::Expr(expr) => {
                    collect_logs_source_metadata_from_expr(expr, &scope, usage);
                }
            },
            SelectItem::UnnamedExpr(expr) => {
                collect_logs_source_metadata_from_expr(expr, &scope, usage);
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                collect_logs_source_metadata_from_expr(expr, &scope, usage);
            }
        }
    }

    if let Some(ref selection) = select.selection {
        collect_logs_source_metadata_from_expr(selection, &scope, usage);
    }

    if let sqlast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for expr in exprs {
            collect_logs_source_metadata_from_expr(expr, &scope, usage);
        }
    }

    if let Some(ref having) = select.having {
        collect_logs_source_metadata_from_expr(having, &scope, usage);
    }

    for table_with_joins in &select.from {
        collect_logs_source_metadata_from_table_with_joins(
            table_with_joins,
            &scope,
            usage,
            cte_names,
            logs_backed_cte_names,
        );
    }

    for sqlast::NamedWindowDefinition(_, named_expr) in &select.named_window {
        if let sqlast::NamedWindowExpr::WindowSpec(spec) = named_expr {
            for expr in &spec.partition_by {
                collect_logs_source_metadata_from_expr(expr, &scope, usage);
            }
            for order_expr in &spec.order_by {
                collect_logs_source_metadata_from_expr(&order_expr.expr, &scope, usage);
            }
        }
    }
}

fn source_metadata_scope_for_select(
    select: &sqlast::Select,
    usage: &mut SourceMetadataUsage,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
    outer_scope: &SourceMetadataScope,
) -> SourceMetadataScope {
    let mut scope = SourceMetadataScope::with_outer(outer_scope);
    scope.cte_names = cte_names.clone();
    scope.logs_backed_cte_names = logs_backed_cte_names.clone();
    for table_with_joins in &select.from {
        collect_logs_aliases_from_table_with_joins(
            table_with_joins,
            usage,
            &mut scope,
            cte_names,
            logs_backed_cte_names,
        );
    }
    scope
}

fn collect_logs_aliases_from_table_with_joins(
    table_with_joins: &sqlast::TableWithJoins,
    usage: &mut SourceMetadataUsage,
    scope: &mut SourceMetadataScope,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
) {
    collect_logs_aliases_from_table_factor(
        &table_with_joins.relation,
        usage,
        scope,
        cte_names,
        logs_backed_cte_names,
    );
    for join in &table_with_joins.joins {
        collect_logs_aliases_from_table_factor(
            &join.relation,
            usage,
            scope,
            cte_names,
            logs_backed_cte_names,
        );
    }
}

fn collect_logs_aliases_from_table_factor(
    factor: &sqlast::TableFactor,
    usage: &mut SourceMetadataUsage,
    scope: &mut SourceMetadataScope,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
) {
    match factor {
        sqlast::TableFactor::Table { name, alias, .. } => {
            scope.visible_relations = scope.visible_relations.saturating_add(1);
            if table_name_exposes_logs_relation(name, cte_names, logs_backed_cte_names) {
                scope.logs_relations = scope.logs_relations.saturating_add(1);
                if let Some(relation_name) = object_name_last_ident(name) {
                    scope.logs_aliases.insert(relation_name.to_string());
                }
                if let Some(alias) = alias {
                    scope.logs_aliases.insert(alias.name.value.clone());
                }
            }
        }
        sqlast::TableFactor::Derived {
            subquery, alias, ..
        } => {
            scope.visible_relations = scope.visible_relations.saturating_add(1);
            collect_logs_source_metadata_from_query_scoped(
                subquery,
                usage,
                cte_names,
                logs_backed_cte_names,
                scope,
            );
            if query_exposes_logs_relation(subquery, cte_names, logs_backed_cte_names) {
                scope.logs_relations = scope.logs_relations.saturating_add(1);
                if let Some(alias) = alias {
                    scope.logs_aliases.insert(alias.name.value.clone());
                }
            }
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_logs_aliases_from_table_with_joins(
                table_with_joins,
                usage,
                scope,
                cte_names,
                logs_backed_cte_names,
            );
        }
        sqlast::TableFactor::Pivot { table, .. }
        | sqlast::TableFactor::Unpivot { table, .. }
        | sqlast::TableFactor::MatchRecognize { table, .. } => {
            collect_logs_aliases_from_table_factor(
                table,
                usage,
                scope,
                cte_names,
                logs_backed_cte_names,
            );
        }
        _ => {
            scope.visible_relations = scope.visible_relations.saturating_add(1);
        }
    }
}

fn query_exposes_logs_relation(
    query: &sqlast::Query,
    inherited_cte_names: &HashSet<String>,
    inherited_logs_backed_cte_names: &HashSet<String>,
) -> bool {
    let cte_names = cte_names_for_query(query);
    let mut accumulated_cte_names = inherited_cte_names.clone();
    let mut accumulated_logs_backed_cte_names = inherited_logs_backed_cte_names.clone();
    if let Some(ref with) = query.with {
        for cte in &with.cte_tables {
            if query_exposes_logs_relation(
                &cte.query,
                &accumulated_cte_names,
                &accumulated_logs_backed_cte_names,
            ) {
                accumulated_logs_backed_cte_names.insert(cte.alias.name.value.clone());
            }
            accumulated_cte_names.insert(cte.alias.name.value.clone());
        }
    }
    let active_cte_names = combined_cte_names(inherited_cte_names, &cte_names);
    set_expr_exposes_logs_relation(
        query.body.as_ref(),
        &active_cte_names,
        &accumulated_logs_backed_cte_names,
    )
}

fn set_expr_exposes_logs_relation(
    set_expr: &SetExpr,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
) -> bool {
    match set_expr {
        SetExpr::Select(select) => select.from.iter().any(|table_with_joins| {
            table_with_joins_exposes_logs_relation(
                table_with_joins,
                cte_names,
                logs_backed_cte_names,
            )
        }),
        SetExpr::Query(query) => {
            query_exposes_logs_relation(query, cte_names, logs_backed_cte_names)
        }
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_exposes_logs_relation(left, cte_names, logs_backed_cte_names)
                || set_expr_exposes_logs_relation(right, cte_names, logs_backed_cte_names)
        }
        _ => false,
    }
}

fn table_with_joins_exposes_logs_relation(
    table_with_joins: &sqlast::TableWithJoins,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
) -> bool {
    table_factor_exposes_logs_relation(&table_with_joins.relation, cte_names, logs_backed_cte_names)
        || table_with_joins.joins.iter().any(|join| {
            table_factor_exposes_logs_relation(&join.relation, cte_names, logs_backed_cte_names)
        })
}

fn table_factor_exposes_logs_relation(
    factor: &sqlast::TableFactor,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
) -> bool {
    match factor {
        sqlast::TableFactor::Table { name, .. } => {
            table_name_exposes_logs_relation(name, cte_names, logs_backed_cte_names)
        }
        sqlast::TableFactor::Derived { subquery, .. } => {
            query_exposes_logs_relation(subquery, cte_names, logs_backed_cte_names)
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_with_joins_exposes_logs_relation(
            table_with_joins,
            cte_names,
            logs_backed_cte_names,
        ),
        sqlast::TableFactor::Pivot { table, .. }
        | sqlast::TableFactor::Unpivot { table, .. }
        | sqlast::TableFactor::MatchRecognize { table, .. } => {
            table_factor_exposes_logs_relation(table, cte_names, logs_backed_cte_names)
        }
        _ => false,
    }
}

fn collect_logs_source_metadata_from_table_with_joins(
    table_with_joins: &sqlast::TableWithJoins,
    scope: &SourceMetadataScope,
    usage: &mut SourceMetadataUsage,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
) {
    collect_logs_source_metadata_from_table_factor(
        &table_with_joins.relation,
        scope,
        usage,
        cte_names,
        logs_backed_cte_names,
    );
    let mut accumulated_has_logs = table_factor_exposes_logs_relation(
        &table_with_joins.relation,
        cte_names,
        logs_backed_cte_names,
    );
    for join in &table_with_joins.joins {
        collect_logs_source_metadata_from_table_factor(
            &join.relation,
            scope,
            usage,
            cte_names,
            logs_backed_cte_names,
        );
        let right_has_logs =
            table_factor_exposes_logs_relation(&join.relation, cte_names, logs_backed_cte_names);
        if let sqlast::JoinOperator::AsOf {
            match_condition, ..
        } = &join.join_operator
        {
            collect_logs_source_metadata_from_expr(match_condition, scope, usage);
        }
        if let Some(constraint) = extract_join_constraint(&join.join_operator) {
            collect_logs_source_metadata_from_join_constraint(
                constraint,
                scope,
                usage,
                accumulated_has_logs || right_has_logs,
            );
        }
        accumulated_has_logs |= right_has_logs;
    }
}

fn collect_logs_source_metadata_from_table_factor(
    factor: &sqlast::TableFactor,
    scope: &SourceMetadataScope,
    usage: &mut SourceMetadataUsage,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
) {
    match factor {
        sqlast::TableFactor::Derived { subquery, .. } => {
            collect_logs_source_metadata_from_query_scoped(
                subquery,
                usage,
                cte_names,
                logs_backed_cte_names,
                scope,
            );
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_logs_source_metadata_from_table_with_joins(
                table_with_joins,
                scope,
                usage,
                cte_names,
                logs_backed_cte_names,
            );
        }
        sqlast::TableFactor::Pivot {
            table,
            value_source,
            ..
        } => {
            collect_logs_source_metadata_from_table_factor(
                table,
                scope,
                usage,
                cte_names,
                logs_backed_cte_names,
            );
            if let sqlast::PivotValueSource::Subquery(query) = value_source {
                collect_logs_source_metadata_from_query_scoped(
                    query,
                    usage,
                    cte_names,
                    logs_backed_cte_names,
                    scope,
                );
            }
        }
        sqlast::TableFactor::Unpivot { table, .. }
        | sqlast::TableFactor::MatchRecognize { table, .. } => {
            collect_logs_source_metadata_from_table_factor(
                table,
                scope,
                usage,
                cte_names,
                logs_backed_cte_names,
            );
        }
        _ => {}
    }
}

fn collect_logs_source_metadata_from_join_constraint(
    constraint: &sqlast::JoinConstraint,
    scope: &SourceMetadataScope,
    usage: &mut SourceMetadataUsage,
    is_logs_join: bool,
) {
    match constraint {
        sqlast::JoinConstraint::On(expr) => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
        }
        sqlast::JoinConstraint::Using(cols) => {
            if is_logs_join {
                for obj_name in cols {
                    if let Some(part) = obj_name.0.last()
                        && let Some(ident) = part.as_ident()
                    {
                        collect_source_metadata_name(&ident.value, usage);
                    }
                }
            }
        }
        sqlast::JoinConstraint::Natural | sqlast::JoinConstraint::None => {}
    }
}

fn collect_logs_source_metadata_from_expr(
    expr: &SqlExpr,
    scope: &SourceMetadataScope,
    usage: &mut SourceMetadataUsage,
) {
    match expr {
        SqlExpr::Identifier(ident) if scope.bare_identifiers_resolve_to_logs() => {
            collect_source_metadata_name(&ident.value, usage);
        }
        SqlExpr::Identifier(_) => {}
        SqlExpr::CompoundIdentifier(parts) => {
            if let Some(last) = parts.last() {
                let qualifier_matches = parts
                    .iter()
                    .rev()
                    .skip(1)
                    .any(|part| scope.matches_logs_qualifier(&part.value));
                if qualifier_matches {
                    collect_source_metadata_name(&last.value, usage);
                }
            }
        }
        SqlExpr::BinaryOp { left, right, .. }
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right) => {
            collect_logs_source_metadata_from_expr(left, scope, usage);
            collect_logs_source_metadata_from_expr(right, scope, usage);
        }
        SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Nested(expr)
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::IsTrue(expr)
        | SqlExpr::IsNotTrue(expr)
        | SqlExpr::IsFalse(expr)
        | SqlExpr::IsNotFalse(expr)
        | SqlExpr::IsUnknown(expr)
        | SqlExpr::IsNotUnknown(expr)
        | SqlExpr::Cast { expr, .. }
        | SqlExpr::Extract { expr, .. }
        | SqlExpr::Ceil { expr, .. }
        | SqlExpr::Floor { expr, .. }
        | SqlExpr::Convert { expr, .. }
        | SqlExpr::Collate { expr, .. } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
        }
        SqlExpr::Function(func) => {
            if let sqlast::FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    collect_logs_source_metadata_from_function_arg(arg, scope, usage);
                }
            } else if let sqlast::FunctionArguments::Subquery(subquery) = &func.args {
                collect_logs_source_metadata_from_query_scoped(
                    subquery,
                    usage,
                    &scope.cte_names,
                    &scope.logs_backed_cte_names,
                    scope,
                );
            }
            if let Some(sqlast::WindowType::WindowSpec(spec)) = &func.over {
                for expr in &spec.partition_by {
                    collect_logs_source_metadata_from_expr(expr, scope, usage);
                }
                for order_expr in &spec.order_by {
                    collect_logs_source_metadata_from_expr(&order_expr.expr, scope, usage);
                }
            }
        }
        SqlExpr::SimilarTo { expr, pattern, .. }
        | SqlExpr::Like { expr, pattern, .. }
        | SqlExpr::ILike { expr, pattern, .. } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
            collect_logs_source_metadata_from_expr(pattern, scope, usage);
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
            collect_logs_source_metadata_from_expr(low, scope, usage);
            collect_logs_source_metadata_from_expr(high, scope, usage);
        }
        SqlExpr::InList { expr, list, .. } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
            for item in list {
                collect_logs_source_metadata_from_expr(item, scope, usage);
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_logs_source_metadata_from_expr(operand, scope, usage);
            }
            for when in conditions {
                collect_logs_source_metadata_from_expr(&when.condition, scope, usage);
                collect_logs_source_metadata_from_expr(&when.result, scope, usage);
            }
            if let Some(expr) = else_result {
                collect_logs_source_metadata_from_expr(expr, scope, usage);
            }
        }
        SqlExpr::Trim {
            expr, trim_what, ..
        } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
            if let Some(trim_what) = trim_what {
                collect_logs_source_metadata_from_expr(trim_what, scope, usage);
            }
        }
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
            if let Some(expr) = substring_from {
                collect_logs_source_metadata_from_expr(expr, scope, usage);
            }
            if let Some(expr) = substring_for {
                collect_logs_source_metadata_from_expr(expr, scope, usage);
            }
        }
        SqlExpr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
            collect_logs_source_metadata_from_expr(overlay_what, scope, usage);
            collect_logs_source_metadata_from_expr(overlay_from, scope, usage);
            if let Some(expr) = overlay_for {
                collect_logs_source_metadata_from_expr(expr, scope, usage);
            }
        }
        SqlExpr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            collect_logs_source_metadata_from_expr(timestamp, scope, usage);
            collect_logs_source_metadata_from_expr(time_zone, scope, usage);
        }
        SqlExpr::Position { expr, r#in } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
            collect_logs_source_metadata_from_expr(r#in, scope, usage);
        }
        SqlExpr::InSubquery { expr, subquery, .. } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
            collect_logs_source_metadata_from_query_scoped(
                subquery,
                usage,
                &scope.cte_names,
                &scope.logs_backed_cte_names,
                scope,
            );
        }
        SqlExpr::InUnnest {
            expr, array_expr, ..
        } => {
            collect_logs_source_metadata_from_expr(expr, scope, usage);
            collect_logs_source_metadata_from_expr(array_expr, scope, usage);
        }
        SqlExpr::Exists { subquery, .. } | SqlExpr::Subquery(subquery) => {
            collect_logs_source_metadata_from_query_scoped(
                subquery,
                usage,
                &scope.cte_names,
                &scope.logs_backed_cte_names,
                scope,
            );
        }
        _ => {}
    }
}

fn collect_logs_source_metadata_from_function_arg(
    arg: &sqlast::FunctionArg,
    scope: &SourceMetadataScope,
    usage: &mut SourceMetadataUsage,
) {
    match arg {
        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(expr))
        | sqlast::FunctionArg::Named {
            arg: sqlast::FunctionArgExpr::Expr(expr),
            ..
        } => collect_logs_source_metadata_from_expr(expr, scope, usage),
        sqlast::FunctionArg::ExprNamed { name, arg, .. } => {
            collect_logs_source_metadata_from_expr(name, scope, usage);
            if let sqlast::FunctionArgExpr::Expr(expr) = arg {
                collect_logs_source_metadata_from_expr(expr, scope, usage);
            }
        }
        _ => {}
    }
}

fn collect_source_metadata_name(name: &str, usage: &mut SourceMetadataUsage) {
    if is_source_metadata_name(name) {
        usage.referenced_columns.insert(name.to_string());
    }
}

fn is_source_metadata_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(field_names::SOURCE_ID)
}

fn source_metadata_plan_for_names(names: &HashSet<String>) -> SourceMetadataPlan {
    let references = |target: &str| names.iter().any(|name| name.eq_ignore_ascii_case(target));

    SourceMetadataPlan {
        has_source_id: references(field_names::SOURCE_ID),
        source_path: SourcePathColumn::default(),
    }
}

fn table_name_exposes_logs_relation(
    name: &sqlast::ObjectName,
    cte_names: &HashSet<String>,
    logs_backed_cte_names: &HashSet<String>,
) -> bool {
    object_name_matches_cte_name(name, logs_backed_cte_names)
        || (object_name_last_eq(name, "logs") && !object_name_is_shadowed_cte(name, cte_names))
}

fn object_name_matches_cte_name(name: &sqlast::ObjectName, cte_names: &HashSet<String>) -> bool {
    name.0.len() == 1
        && object_name_last_ident(name).is_some_and(|ident| {
            cte_names
                .iter()
                .any(|cte_name| cte_name.eq_ignore_ascii_case(ident))
        })
}

fn object_name_last_ident(name: &sqlast::ObjectName) -> Option<&str> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.as_str())
}

fn object_name_last_eq(name: &sqlast::ObjectName, expected: &str) -> bool {
    object_name_last_ident(name).is_some_and(|ident| ident.eq_ignore_ascii_case(expected))
}

fn object_name_is_shadowed_cte(name: &sqlast::ObjectName, cte_names: &HashSet<String>) -> bool {
    object_name_matches_cte_name(name, cte_names)
}

fn cte_names_for_query(query: &sqlast::Query) -> HashSet<String> {
    query
        .with
        .as_ref()
        .map(|with| {
            with.cte_tables
                .iter()
                .map(|cte| cte.alias.name.value.clone())
                .collect()
        })
        .unwrap_or_default()
}

fn combined_cte_names(
    inherited_cte_names: &HashSet<String>,
    local_cte_names: &HashSet<String>,
) -> HashSet<String> {
    let mut combined = inherited_cte_names.clone();
    combined.extend(local_cte_names.iter().cloned());
    combined
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs(sql: &str) -> HashSet<String> {
        QueryAnalyzer::new(sql).unwrap().referenced_columns
    }

    #[test]
    fn cte_columns_are_collected() {
        let cols =
            refs("WITH cte AS (SELECT severity, facility FROM logs) SELECT severity FROM cte");
        assert!(cols.contains("severity"), "missing severity: {cols:?}");
        assert!(
            cols.contains("facility"),
            "missing facility from CTE body: {cols:?}"
        );
    }

    #[test]
    fn multiple_ctes_are_walked() {
        let cols = refs(
            "WITH a AS (SELECT host FROM logs), b AS (SELECT pid FROM logs) \
             SELECT host FROM a",
        );
        assert!(cols.contains("host"), "missing host: {cols:?}");
        assert!(
            cols.contains("pid"),
            "missing pid from second CTE: {cols:?}"
        );
    }

    #[test]
    fn in_subquery_body_is_walked() {
        let cols = refs("SELECT level FROM logs WHERE level IN (SELECT severity FROM alerts)");
        assert!(cols.contains("level"), "missing level: {cols:?}");
        assert!(
            cols.contains("severity"),
            "missing severity from IN subquery: {cols:?}"
        );
    }

    #[test]
    fn exists_subquery_is_walked() {
        let cols = refs(
            "SELECT host FROM logs WHERE EXISTS (SELECT 1 FROM alerts WHERE alerts.src = logs.src)",
        );
        assert!(cols.contains("host"), "missing host: {cols:?}");
        assert!(
            cols.contains("src"),
            "missing src from EXISTS subquery: {cols:?}"
        );
    }

    #[test]
    fn not_exists_subquery_is_walked() {
        let cols = refs(
            "SELECT host FROM logs \
             WHERE NOT EXISTS (SELECT 1 FROM alerts WHERE alerts.pid = logs.pid)",
        );
        assert!(
            cols.contains("pid"),
            "missing pid from NOT EXISTS subquery: {cols:?}"
        );
    }

    #[test]
    fn scalar_subquery_is_walked() {
        let cols = refs("SELECT host, (SELECT MAX(severity) FROM alerts) AS max_sev FROM logs");
        assert!(cols.contains("host"), "missing host: {cols:?}");
        assert!(
            cols.contains("severity"),
            "missing severity from scalar subquery: {cols:?}"
        );
    }

    #[test]
    fn in_subquery_select_star_marks_extract_all() {
        let analyzer =
            QueryAnalyzer::new("SELECT level FROM logs WHERE level IN (SELECT * FROM alerts)")
                .unwrap();
        assert!(
            analyzer.uses_select_star,
            "SELECT * in IN subquery must mark extract_all"
        );
    }

    #[test]
    fn exists_subquery_select_star_marks_extract_all() {
        let analyzer = QueryAnalyzer::new(
            "SELECT host FROM logs WHERE EXISTS (SELECT * FROM alerts WHERE alerts.pid = logs.pid)",
        )
        .unwrap();
        assert!(
            analyzer.uses_select_star,
            "SELECT * in EXISTS subquery must mark extract_all"
        );
    }

    #[test]
    fn scalar_subquery_select_star_marks_extract_all() {
        let analyzer =
            QueryAnalyzer::new("SELECT host, (SELECT * FROM alerts) AS alert FROM logs").unwrap();
        assert!(
            analyzer.uses_select_star,
            "SELECT * in scalar subquery must mark extract_all"
        );
    }

    #[test]
    fn source_path_not_required_for_unrelated_projection() {
        let analyzer = QueryAnalyzer::new("SELECT level FROM logs WHERE status = '500'").unwrap();
        assert!(!analyzer.source_path_required());
        assert_eq!(
            analyzer.source_metadata_plan(),
            SourceMetadataPlan::default()
        );
    }

    #[test]
    fn source_id_required_for_explicit_projection() {
        let analyzer = QueryAnalyzer::new("SELECT __source_id FROM logs").unwrap();
        let plan = analyzer.source_metadata_plan();
        assert!(plan.has_source_id);
        assert!(!plan.has_source_path());
    }

    #[test]
    fn explicit_source_metadata_plan_excludes_wildcard_projection() {
        let analyzer = QueryAnalyzer::new("SELECT * FROM logs").unwrap();
        assert_eq!(
            analyzer.explicit_source_metadata_plan(),
            SourceMetadataPlan::default()
        );
    }

    #[test]
    fn explicit_source_metadata_plan_includes_join_reference() {
        let analyzer = QueryAnalyzer::new(
            "SELECT l.msg FROM logs l \
             LEFT JOIN sources s ON l.__source_id = s.source_id",
        )
        .unwrap();
        assert!(analyzer.explicit_source_metadata_plan().has_source_id);
    }

    #[test]
    fn source_metadata_plan_ignores_public_style_columns() {
        let analyzer = QueryAnalyzer::new(r#"SELECT "file.path" FROM logs"#).unwrap();
        assert_eq!(
            analyzer.source_metadata_plan(),
            SourceMetadataPlan::default()
        );
    }

    #[test]
    fn explicit_source_metadata_plan_uses_logs_alias_only() {
        let analyzer = QueryAnalyzer::new(
            "SELECT l.msg FROM logs l \
             LEFT JOIN sources s ON s.other_id = k.__source_id",
        )
        .unwrap();
        assert_eq!(
            analyzer.explicit_source_metadata_plan(),
            SourceMetadataPlan::default()
        );

        let analyzer = QueryAnalyzer::new(
            "SELECT l.msg FROM logs l \
             LEFT JOIN sources s ON l.__source_id = s.source_id",
        )
        .unwrap();
        assert!(analyzer.explicit_source_metadata_plan().has_source_id);
    }

    #[test]
    fn explicit_source_metadata_plan_ignores_cte_shadowing_logs() {
        let analyzer = QueryAnalyzer::new(
            "WITH logs AS (SELECT 1 AS __source_id) SELECT __source_id FROM logs",
        )
        .unwrap();
        assert_eq!(
            analyzer.explicit_source_metadata_plan(),
            SourceMetadataPlan::default()
        );
        assert_eq!(
            analyzer.source_metadata_plan(),
            SourceMetadataPlan::default()
        );
    }

    #[test]
    fn explicit_source_metadata_plan_tracks_logs_backed_cte_alias() {
        let analyzer =
            QueryAnalyzer::new("WITH c AS (SELECT * FROM logs) SELECT c.__source_id FROM c")
                .unwrap();
        assert!(analyzer.explicit_source_metadata_plan().has_source_id);

        let analyzer =
            QueryAnalyzer::new("WITH c AS (SELECT * FROM alerts) SELECT c.__source_id FROM c")
                .unwrap();
        assert_eq!(
            analyzer.explicit_source_metadata_plan(),
            SourceMetadataPlan::default()
        );
    }

    // -------------------------------------------------------------------
    // ScanPredicate extraction tests
    // -------------------------------------------------------------------

    fn predicate_for(sql: &str) -> Option<ScanPredicate> {
        QueryAnalyzer::new(sql).unwrap().extract_scan_predicate()
    }

    #[test]
    fn extract_eq_string_predicate() {
        let pred = predicate_for("SELECT * FROM logs WHERE level = 'error'");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::Compare { field, op, value } => {
                assert_eq!(field, "level");
                assert_eq!(op, CmpOp::Eq);
                assert_eq!(value, ScalarValue::Str("error".to_string()));
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    #[test]
    fn extract_gt_int_predicate() {
        let pred = predicate_for("SELECT * FROM logs WHERE status >= 500");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::Compare { field, op, value } => {
                assert_eq!(field, "status");
                assert_eq!(op, CmpOp::Ge);
                assert_eq!(value, ScalarValue::Int(500));
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    #[test]
    fn extract_and_chain() {
        let pred = predicate_for("SELECT * FROM logs WHERE level = 'error' AND status >= 500");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::And(preds) => {
                assert_eq!(preds.len(), 2);
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn extract_is_null() {
        let pred = predicate_for("SELECT * FROM logs WHERE trace_id IS NULL");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::IsNull { field, negated } => {
                assert_eq!(field, "trace_id");
                assert!(!negated);
            }
            other => panic!("expected IsNull, got {other:?}"),
        }
    }

    #[test]
    fn extract_is_not_null() {
        let pred = predicate_for("SELECT * FROM logs WHERE trace_id IS NOT NULL");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::IsNull { field, negated } => {
                assert_eq!(field, "trace_id");
                assert!(negated);
            }
            other => panic!("expected IsNull(negated), got {other:?}"),
        }
    }

    #[test]
    fn extract_reversed_operand_order() {
        let pred = predicate_for("SELECT * FROM logs WHERE 500 <= status");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::Compare { field, op, value } => {
                assert_eq!(field, "status");
                assert_eq!(op, CmpOp::Ge);
                assert_eq!(value, ScalarValue::Int(500));
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    #[test]
    fn no_predicate_without_where() {
        let pred = predicate_for("SELECT * FROM logs");
        assert!(pred.is_none());
    }

    #[test]
    fn metadata_columns_are_not_pushed() {
        assert!(predicate_for(r#"SELECT * FROM logs WHERE "file.path" = '/tmp/x.log'"#).is_none());
        assert!(predicate_for("SELECT * FROM logs WHERE __source_id = 7").is_none());
        assert!(predicate_for("SELECT * FROM logs WHERE _stream = 'stdout'").is_none());
        assert!(predicate_for(r#"SELECT * FROM logs WHERE "@timestamp" IS NOT NULL"#).is_none());
        assert!(predicate_for("SELECT * FROM logs WHERE _timestamp IS NOT NULL").is_none());
    }

    #[test]
    fn metadata_columns_are_not_pushed_case_insensitively() {
        assert!(predicate_for(r#"SELECT * FROM logs WHERE "FILE.PATH" = '/tmp/x.log'"#).is_none());
        assert!(predicate_for("SELECT * FROM logs WHERE __SOURCE_ID = 7").is_none());
        assert!(predicate_for("SELECT * FROM logs WHERE _STREAM = 'stdout'").is_none());
        assert!(predicate_for(r#"SELECT * FROM logs WHERE "@TIMESTAMP" IS NOT NULL"#).is_none());
        assert!(predicate_for("SELECT * FROM logs WHERE _TIMESTAMP IS NOT NULL").is_none());
    }

    #[test]
    fn unpushable_or_different_ops_returns_none() {
        // OR with non-Eq comparisons can't merge to InList.
        let pred = predicate_for("SELECT * FROM logs WHERE status > 500 OR status < 200");
        assert!(pred.is_none());
    }

    #[test]
    fn and_with_unpushable_conjunct_pushes_partial() {
        // Use UPPER(msg), a function call that is truly unpushable, instead of
        // LIKE '%fail%' which is correctly pushed as Contains.
        let pred =
            predicate_for("SELECT * FROM logs WHERE level = 'error' AND UPPER(msg) = 'FAIL'");
        // UPPER() is not pushable, but level = 'error' is.
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::Compare { field, .. } => {
                assert_eq!(field, "level");
            }
            other => panic!("expected Compare (partial push), got {other:?}"),
        }
    }

    #[test]
    fn extract_in_list() {
        let pred = predicate_for("SELECT * FROM logs WHERE level IN ('error', 'fatal', 'warn')");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::InList {
                field,
                values,
                negated,
            } => {
                assert_eq!(field, "level");
                assert_eq!(values.len(), 3);
                assert!(!negated);
            }
            other => panic!("expected InList, got {other:?}"),
        }
    }

    #[test]
    fn extract_not_in_list() {
        let pred = predicate_for("SELECT * FROM logs WHERE level NOT IN ('debug', 'trace')");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::InList {
                field,
                values,
                negated,
            } => {
                assert_eq!(field, "level");
                assert_eq!(values.len(), 2);
                assert!(negated);
            }
            other => panic!("expected InList(negated), got {other:?}"),
        }
    }

    #[test]
    fn extract_or_same_field_eq_becomes_in_list() {
        let pred = predicate_for("SELECT * FROM logs WHERE level = 'error' OR level = 'fatal'");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::InList {
                field,
                values,
                negated,
            } => {
                assert_eq!(field, "level");
                assert_eq!(values.len(), 2);
                assert!(!negated);
            }
            other => panic!("expected InList from OR merge, got {other:?}"),
        }
    }

    #[test]
    fn extract_or_different_fields_not_pushed() {
        let pred = predicate_for("SELECT * FROM logs WHERE level = 'error' OR status = 500");
        // Different fields in OR — can't merge to InList.
        assert!(pred.is_none());
    }

    #[test]
    fn extract_like_prefix() {
        let pred = predicate_for("SELECT * FROM logs WHERE msg LIKE 'ERROR%'");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::StartsWith { field, prefix } => {
                assert_eq!(field, "msg");
                assert_eq!(prefix, "ERROR");
            }
            other => panic!("expected StartsWith, got {other:?}"),
        }
    }

    #[test]
    fn extract_like_contains() {
        let pred = predicate_for("SELECT * FROM logs WHERE msg LIKE '%timeout%'");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::Contains { field, substring } => {
                assert_eq!(field, "msg");
                assert_eq!(substring, "timeout");
            }
            other => panic!("expected Contains, got {other:?}"),
        }
    }

    #[test]
    fn extract_like_complex_not_pushed() {
        let pred = predicate_for("SELECT * FROM logs WHERE msg LIKE '%error%timeout%'");
        // Multiple wildcards — not pushable.
        assert!(pred.is_none());
    }

    #[test]
    fn extract_not_equal() {
        let pred = predicate_for("SELECT * FROM logs WHERE level <> 'debug'");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::Compare { field, op, value } => {
                assert_eq!(field, "level");
                assert_eq!(op, CmpOp::Ne);
                assert_eq!(value, ScalarValue::Str("debug".to_string()));
            }
            other => panic!("expected Compare(Ne), got {other:?}"),
        }
    }

    #[test]
    fn qualified_column_not_pushed() {
        // Qualified column reference (e.g., from a join) should NOT be pushed.
        let pred = predicate_for(
            "SELECT * FROM logs JOIN env ON logs.id = env.id WHERE env.level = 'error'",
        );
        // env.level is a CompoundIdentifier — should not be pushed.
        assert!(pred.is_none());
    }

    #[test]
    fn file_path_source_metadata_not_pushed() {
        assert!(predicate_for(r#"SELECT * FROM logs WHERE "file.path" = '/tmp/x.log'"#).is_none());
    }

    #[test]
    fn source_id_metadata_not_pushed() {
        assert!(predicate_for("SELECT * FROM logs WHERE __source_id = 7").is_none());
    }

    #[test]
    fn source_id_metadata_not_pushed_case_insensitively() {
        assert!(predicate_for("SELECT * FROM logs WHERE __SOURCE_ID = 7").is_none());
    }

    #[test]
    fn cri_stream_metadata_not_pushed() {
        assert!(predicate_for("SELECT * FROM logs WHERE _stream = 'stdout'").is_none());
    }

    #[test]
    fn cri_stream_metadata_not_pushed_case_insensitively() {
        assert!(predicate_for("SELECT * FROM logs WHERE _STREAM = 'stdout'").is_none());
    }

    #[test]
    fn cri_timestamp_metadata_not_pushed() {
        assert!(predicate_for("SELECT * FROM logs WHERE _timestamp IS NOT NULL").is_none());
    }

    #[test]
    fn file_path_metadata_not_pushed_case_insensitively() {
        assert!(predicate_for(r#"SELECT * FROM logs WHERE "FILE.PATH" = '/tmp/x.log'"#).is_none());
    }

    #[test]
    fn negative_number_literal() {
        let pred = predicate_for("SELECT * FROM logs WHERE temp > -10");
        assert!(pred.is_some());
        match pred.unwrap() {
            ScanPredicate::Compare { field, op, value } => {
                assert_eq!(field, "temp");
                assert_eq!(op, CmpOp::Gt);
                assert_eq!(value, ScalarValue::Int(-10));
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }
}
