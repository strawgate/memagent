// rewriter.rs — SQL rewriter that translates user-friendly bare-column SQL
// to typed-column SQL that the scanner's typed Arrow schema can handle.
//
// The scanner produces columns named `{field}_str`, `{field}_int`, and
// `{field}_float`. Users naturally write `level = 'ERROR'` or `status > 400`,
// but DataFusion needs `level_str = 'ERROR'` or `status_int > 400`.
//
// Rewrite rules:
//  1. Bare column in SELECT: `duration_ms`
//     → `COALESCE(CAST(duration_ms_int AS VARCHAR), duration_ms_str) AS duration_ms`
//  2. Bare column in WHERE with string literal: `level = 'ERROR'`
//     → `level_str = 'ERROR'`
//  3. Bare column in WHERE with numeric literal: `status > 400`
//     → `status_int > 400`
//  4. `int(x)` call where `x` is a known field:
//     → `COALESCE(x_int, TRY_CAST(x_str AS BIGINT))`

use std::collections::HashMap;

use arrow::datatypes::Schema;
use datafusion::sql::sqlparser::ast::{
    self as sqlast, CastKind, Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, SelectItem, SetExpr, Statement,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

// ---------------------------------------------------------------------------
// FieldTypeMap
// ---------------------------------------------------------------------------

/// Which Arrow type variants the scanner has produced for a JSON field.
///
/// When the scanner encounters a JSON field that appears as multiple types
/// across log lines, it creates separate columns for each type: `{field}_str`,
/// `{field}_int`, `{field}_float`. This struct records which variants exist.
#[derive(Debug, Clone, Default)]
pub struct FieldTypes {
    /// The `{field}_str` column exists in the schema.
    pub has_str: bool,
    /// The `{field}_int` column exists in the schema.
    pub has_int: bool,
    /// The `{field}_float` column exists in the schema.
    pub has_float: bool,
}

/// Maps bare JSON field names (e.g., `"level"`) to the type variants
/// present in the current Arrow schema (e.g., `_str`, `_int`, `_float`).
///
/// Build one from a schema using [`field_type_map_from_schema`].
pub type FieldTypeMap = HashMap<String, FieldTypes>;

/// Build a [`FieldTypeMap`] from an Arrow [`Schema`].
///
/// Scans column names for the `_str`, `_int`, and `_float` suffixes and
/// groups them by their bare field name.
///
/// # Example
///
/// A schema with columns `level_str`, `status_int`, `status_str`,
/// `latency_ms_float` produces:
/// ```text
/// { "level"      => { has_str: true, has_int: false, has_float: false },
///   "status"     => { has_str: true, has_int: true,  has_float: false },
///   "latency_ms" => { has_str: false, has_int: false, has_float: true } }
/// ```
pub fn field_type_map_from_schema(schema: &Schema) -> FieldTypeMap {
    let mut map: FieldTypeMap = HashMap::new();
    for field in schema.fields() {
        let name = field.name().as_str();
        if let Some(base) = name.strip_suffix("_str").filter(|b| !b.is_empty()) {
            map.entry(base.to_string()).or_default().has_str = true;
        } else if let Some(base) = name.strip_suffix("_int").filter(|b| !b.is_empty()) {
            map.entry(base.to_string()).or_default().has_int = true;
        } else if let Some(base) = name.strip_suffix("_float").filter(|b| !b.is_empty()) {
            map.entry(base.to_string()).or_default().has_float = true;
        }
    }
    map
}

// ---------------------------------------------------------------------------
// rewrite_sql
// ---------------------------------------------------------------------------

/// Rewrite user-friendly SQL to typed-column SQL.
///
/// Translates bare field references (columns without a `_str`/`_int`/`_float`
/// suffix) to their typed equivalents when the field is known in `fields`.
/// Columns that already carry a type suffix or are not in `fields` are left
/// unchanged.
///
/// # Rewrite rules
///
/// 1. **Bare column in SELECT** — e.g. `SELECT duration_ms FROM logs`
///    becomes `SELECT COALESCE(CAST(duration_ms_int AS VARCHAR), duration_ms_str) AS duration_ms FROM logs`
///
/// 2. **String comparison in WHERE** — e.g. `WHERE level = 'ERROR'`
///    becomes `WHERE level_str = 'ERROR'` (uses `_str` variant)
///
/// 3. **Numeric comparison in WHERE** — e.g. `WHERE status > 400`
///    becomes `WHERE status_int > 400` (uses `_int` variant, or `_float` if
///    only float exists)
///
/// 4. **`int(x)` call** — e.g. `int(duration_ms)` becomes
///    `COALESCE(duration_ms_int, TRY_CAST(duration_ms_str AS BIGINT))`
///
/// # Errors
///
/// Returns an error if the SQL cannot be parsed or contains more than one
/// statement.
pub fn rewrite_sql(user_sql: &str, fields: &FieldTypeMap) -> Result<String, String> {
    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, user_sql).map_err(|e| format!("SQL parse error: {e}"))?;

    if statements.len() != 1 {
        return Err("Expected exactly one SQL statement".to_string());
    }

    rewrite_statement(&mut statements[0], fields);
    Ok(statements[0].to_string())
}

// ---------------------------------------------------------------------------
// Statement-level rewriting
// ---------------------------------------------------------------------------

fn rewrite_statement(stmt: &mut Statement, fields: &FieldTypeMap) {
    if let Statement::Query(query) = stmt
        && let SetExpr::Select(select) = query.body.as_mut()
    {
        // Rule 1 + Rule 4 in SELECT projection.
        let projection = std::mem::take(&mut select.projection);
        select.projection = projection
            .into_iter()
            .map(|item| rewrite_select_item(item, fields))
            .collect();

        // Rules 2, 3, 4: expressions in WHERE clause.
        select.selection = select.selection.take().map(|sel| rewrite_expr(sel, fields));
    }
}

// ---------------------------------------------------------------------------
// SELECT projection rewriting (Rule 1)
// ---------------------------------------------------------------------------

/// Rewrite a single SELECT item.
///
/// - Bare identifier → aliased COALESCE expression (Rule 1).
/// - Other expressions → apply general expression rewriting (Rule 4, etc.).
fn rewrite_select_item(item: SelectItem, fields: &FieldTypeMap) -> SelectItem {
    match item {
        // Bare identifier: `duration_ms` → `COALESCE(...) AS duration_ms`
        SelectItem::UnnamedExpr(expr) => {
            if let SqlExpr::Identifier(ref ident) = expr {
                let name = ident.value.clone();
                if let Some(ft) = fields.get(&name) {
                    return SelectItem::ExprWithAlias {
                        expr: build_select_coalesce(&name, ft),
                        alias: Ident::new(name),
                    };
                }
            }
            // Other unnamed expressions: apply general rewriting (Rule 4, etc.).
            SelectItem::UnnamedExpr(rewrite_expr(expr, fields))
        }
        // Aliased expression: `x AS alias`
        SelectItem::ExprWithAlias { expr, alias } => {
            // Bare identifier aliased: `duration_ms AS dur` → COALESCE expression.
            if let SqlExpr::Identifier(ref ident) = expr {
                let name = ident.value.clone();
                if let Some(ft) = fields.get(&name) {
                    return SelectItem::ExprWithAlias {
                        expr: build_select_coalesce(&name, ft),
                        alias,
                    };
                }
            }
            // Other aliased expressions: apply general rewriting.
            SelectItem::ExprWithAlias {
                expr: rewrite_expr(expr, fields),
                alias,
            }
        }
        // Wildcards and other items are not rewritten.
        other => other,
    }
}

/// Build the COALESCE expression for a bare SELECT column reference.
///
/// Priority: int → str (for mixed schemas), or whichever variants exist.
fn build_select_coalesce(base: &str, ft: &FieldTypes) -> SqlExpr {
    match (ft.has_int, ft.has_float, ft.has_str) {
        // int + str: COALESCE(CAST(x_int AS VARCHAR), x_str)
        (true, _, true) => {
            let cast_int = make_cast(
                SqlExpr::Identifier(Ident::new(format!("{base}_int"))),
                sqlast::DataType::Varchar(None),
            );
            let str_col = SqlExpr::Identifier(Ident::new(format!("{base}_str")));
            make_coalesce(vec![cast_int, str_col])
        }
        // int only: CAST(x_int AS VARCHAR)
        (true, _, false) => make_cast(
            SqlExpr::Identifier(Ident::new(format!("{base}_int"))),
            sqlast::DataType::Varchar(None),
        ),
        // float + str: COALESCE(CAST(x_float AS VARCHAR), x_str)
        (false, true, true) => {
            let cast_float = make_cast(
                SqlExpr::Identifier(Ident::new(format!("{base}_float"))),
                sqlast::DataType::Varchar(None),
            );
            let str_col = SqlExpr::Identifier(Ident::new(format!("{base}_str")));
            make_coalesce(vec![cast_float, str_col])
        }
        // float only: CAST(x_float AS VARCHAR)
        (false, true, false) => make_cast(
            SqlExpr::Identifier(Ident::new(format!("{base}_float"))),
            sqlast::DataType::Varchar(None),
        ),
        // str only: x_str
        (false, false, true) => SqlExpr::Identifier(Ident::new(format!("{base}_str"))),
        // No known variants — leave bare (shouldn't occur with a valid FieldTypeMap).
        (false, false, false) => SqlExpr::Identifier(Ident::new(base.to_string())),
    }
}

// ---------------------------------------------------------------------------
// WHERE clause expression rewriting (Rules 2, 3, 4)
// ---------------------------------------------------------------------------

fn rewrite_expr(expr: SqlExpr, fields: &FieldTypeMap) -> SqlExpr {
    match expr {
        // AND / OR: recurse into both branches.
        SqlExpr::BinaryOp {
            left,
            op: op @ (sqlast::BinaryOperator::And | sqlast::BinaryOperator::Or),
            right,
        } => SqlExpr::BinaryOp {
            left: Box::new(rewrite_expr(*left, fields)),
            op,
            right: Box::new(rewrite_expr(*right, fields)),
        },

        // Comparison operators: apply Rules 2 and 3.
        SqlExpr::BinaryOp { left, op, right } => {
            // Evaluate both rewrites while we still have shared borrows, then
            // consume left/right for the fallback recursive calls.
            let left_rewrite = try_rewrite_col_by_literal(&left, &right, fields);
            let right_rewrite = try_rewrite_col_by_literal(&right, &left, fields);

            let new_left = left_rewrite.unwrap_or_else(|| rewrite_expr(*left, fields));
            // Try to rewrite the right side using the left side as the literal hint
            // (handles reversed comparisons like `'ERROR' = level`).
            let new_right = right_rewrite.unwrap_or_else(|| rewrite_expr(*right, fields));
            SqlExpr::BinaryOp {
                left: Box::new(new_left),
                op,
                right: Box::new(new_right),
            }
        }

        // IN list: `level IN ('ERROR', 'WARN')` → `level_str IN ('ERROR', 'WARN')`
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let all_strings = list.iter().all(|e| {
                matches!(
                    e,
                    SqlExpr::Value(sqlast::Value::SingleQuotedString(_))
                        | SqlExpr::Value(sqlast::Value::DoubleQuotedString(_))
                )
            });
            let new_expr = if all_strings {
                if let SqlExpr::Identifier(ref ident) = *expr {
                    if let Some(ft) = fields.get(&ident.value) {
                        if ft.has_str {
                            Box::new(SqlExpr::Identifier(Ident::new(format!(
                                "{}_str",
                                ident.value
                            ))))
                        } else {
                            expr
                        }
                    } else {
                        expr
                    }
                } else {
                    Box::new(rewrite_expr(*expr, fields))
                }
            } else {
                Box::new(rewrite_expr(*expr, fields))
            };
            SqlExpr::InList {
                expr: new_expr,
                list,
                negated,
            }
        }

        // Rule 4: `int(x)` call → `COALESCE(x_int, TRY_CAST(x_str AS BIGINT))`
        SqlExpr::Function(ref func)
            if func_name_is(func, "int")
                && func_single_bare_arg(func)
                    .and_then(|n| fields.get(n))
                    .is_some() =>
        {
            let name = func_single_bare_arg(func).unwrap().to_string();
            let ft = fields.get(&name).unwrap();
            build_int_coalesce(&name, ft)
        }

        // Parenthesized expression: recurse.
        SqlExpr::Nested(inner) => SqlExpr::Nested(Box::new(rewrite_expr(*inner, fields))),

        // IS NULL / IS NOT NULL: recurse.
        SqlExpr::IsNull(e) => SqlExpr::IsNull(Box::new(rewrite_expr(*e, fields))),
        SqlExpr::IsNotNull(e) => SqlExpr::IsNotNull(Box::new(rewrite_expr(*e, fields))),

        // BETWEEN: `col BETWEEN a AND b` — try to rewrite column.
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            // If `low` and `high` are both numbers, prefer _int.
            let is_numeric = is_numeric_literal(&low) && is_numeric_literal(&high);
            let new_expr = if is_numeric {
                if let SqlExpr::Identifier(ref ident) = *expr {
                    if let Some(ft) = fields.get(&ident.value) {
                        if ft.has_int {
                            Box::new(SqlExpr::Identifier(Ident::new(format!(
                                "{}_int",
                                ident.value
                            ))))
                        } else if ft.has_float {
                            Box::new(SqlExpr::Identifier(Ident::new(format!(
                                "{}_float",
                                ident.value
                            ))))
                        } else {
                            expr
                        }
                    } else {
                        expr
                    }
                } else {
                    Box::new(rewrite_expr(*expr, fields))
                }
            } else {
                Box::new(rewrite_expr(*expr, fields))
            };
            SqlExpr::Between {
                expr: new_expr,
                negated,
                low,
                high,
            }
        }

        // UnaryOp: recurse.
        SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
            op,
            expr: Box::new(rewrite_expr(*expr, fields)),
        },

        // All other expressions: pass through unchanged.
        other => other,
    }
}

/// If `col_expr` is a bare identifier known in `fields` and `literal_expr` is a
/// SQL literal, return the typed column name that should replace `col_expr`.
///
/// Returns `None` if no rewrite is applicable.
fn try_rewrite_col_by_literal(
    col_expr: &SqlExpr,
    literal_expr: &SqlExpr,
    fields: &FieldTypeMap,
) -> Option<SqlExpr> {
    if let SqlExpr::Identifier(ident) = col_expr {
        if let Some(ft) = fields.get(&ident.value) {
            let base = &ident.value;
            match literal_expr {
                // String literal → use _str variant (Rule 2)
                SqlExpr::Value(
                    sqlast::Value::SingleQuotedString(_) | sqlast::Value::DoubleQuotedString(_),
                ) if ft.has_str => Some(SqlExpr::Identifier(Ident::new(format!("{base}_str")))),

                // Numeric literal → prefer _int, fall back to _float (Rule 3)
                SqlExpr::Value(sqlast::Value::Number(_, _)) => {
                    if ft.has_int {
                        Some(SqlExpr::Identifier(Ident::new(format!("{base}_int"))))
                    } else if ft.has_float {
                        Some(SqlExpr::Identifier(Ident::new(format!("{base}_float"))))
                    } else {
                        None
                    }
                }

                _ => None,
            }
        } else {
            None
        }
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Rule 4 helpers: int(x) → COALESCE(x_int, TRY_CAST(x_str AS BIGINT))
// ---------------------------------------------------------------------------

/// Build `COALESCE(x_int, TRY_CAST(x_str AS BIGINT))` for the `int(x)` rule.
fn build_int_coalesce(base: &str, ft: &FieldTypes) -> SqlExpr {
    let mut args: Vec<SqlExpr> = Vec::new();
    if ft.has_int {
        args.push(SqlExpr::Identifier(Ident::new(format!("{base}_int"))));
    }
    if ft.has_str {
        args.push(make_try_cast(
            SqlExpr::Identifier(Ident::new(format!("{base}_str"))),
            sqlast::DataType::BigInt(None),
        ));
    }
    match args.len() {
        0 => SqlExpr::Identifier(Ident::new(base.to_string())),
        1 => args.remove(0),
        _ => make_coalesce(args),
    }
}

// ---------------------------------------------------------------------------
// AST construction helpers
// ---------------------------------------------------------------------------

fn make_coalesce(args: Vec<SqlExpr>) -> SqlExpr {
    let func_args: Vec<FunctionArg> = args
        .into_iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
        .collect();
    SqlExpr::Function(sqlast::Function {
        name: ObjectName(vec![Ident::new("COALESCE")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: func_args,
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn make_cast(expr: SqlExpr, data_type: sqlast::DataType) -> SqlExpr {
    SqlExpr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(expr),
        data_type,
        format: None,
    }
}

fn make_try_cast(expr: SqlExpr, data_type: sqlast::DataType) -> SqlExpr {
    SqlExpr::Cast {
        kind: CastKind::TryCast,
        expr: Box::new(expr),
        data_type,
        format: None,
    }
}

// ---------------------------------------------------------------------------
// Predicate helpers
// ---------------------------------------------------------------------------

/// Check whether a `Function` node has the given name (case-insensitive).
fn func_name_is(func: &sqlast::Function, name: &str) -> bool {
    func.name
        .0
        .last()
        .is_some_and(|i| i.value.eq_ignore_ascii_case(name))
}

/// If the function has exactly one unnamed argument that is a bare identifier
/// (not already typed), return the identifier name. Otherwise return `None`.
fn func_single_bare_arg(func: &sqlast::Function) -> Option<&str> {
    if let FunctionArguments::List(ref arg_list) = func.args
        && arg_list.args.len() == 1
        && let FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(ref ident))) =
            arg_list.args[0]
    {
        return Some(ident.value.as_str());
    }
    None
}

/// Return `true` if the expression is a numeric literal.
fn is_numeric_literal(expr: &SqlExpr) -> bool {
    matches!(expr, SqlExpr::Value(sqlast::Value::Number(_, _)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn map(entries: &[(&str, bool, bool, bool)]) -> FieldTypeMap {
        entries
            .iter()
            .map(|(name, has_str, has_int, has_float)| {
                (
                    name.to_string(),
                    FieldTypes {
                        has_str: *has_str,
                        has_int: *has_int,
                        has_float: *has_float,
                    },
                )
            })
            .collect()
    }

    // --- field_type_map_from_schema ---

    #[test]
    fn test_field_type_map_from_schema() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Schema::new(vec![
            Field::new("level_str", DataType::Utf8, true),
            Field::new("status_int", DataType::Int64, true),
            Field::new("status_str", DataType::Utf8, true),
            Field::new("latency_ms_float", DataType::Float64, true),
        ]);
        let m = field_type_map_from_schema(&schema);

        assert!(m["level"].has_str);
        assert!(!m["level"].has_int);

        assert!(m["status"].has_str);
        assert!(m["status"].has_int);
        assert!(!m["status"].has_float);

        assert!(m["latency_ms"].has_float);
        assert!(!m["latency_ms"].has_str);
    }

    // --- rewrite_sql: passthrough for already-typed columns ---

    #[test]
    fn test_passthrough_typed_columns() {
        let fields = map(&[("level", true, false, false)]);
        // `level_str` already has the suffix — must not be double-rewritten.
        let sql = "SELECT level_str FROM logs WHERE level_str = 'ERROR'";
        let out = rewrite_sql(sql, &fields).unwrap();
        // The column name has a suffix so it's not in FieldTypeMap as "level_str"
        // (the map only contains "level"). The query should be unchanged.
        assert!(
            out.contains("level_str"),
            "typed column should be preserved: {out}"
        );
        assert!(
            !out.contains("level_str_str"),
            "double-suffixing must not occur: {out}"
        );
    }

    // --- Rule 2: WHERE string comparison ---

    #[test]
    fn test_where_string_eq() {
        let fields = map(&[("level", true, false, false)]);
        let out = rewrite_sql("SELECT * FROM logs WHERE level = 'ERROR'", &fields).unwrap();
        assert!(
            out.contains("level_str"),
            "bare string comparison should use _str: {out}"
        );
        assert!(
            !out.contains("level ="),
            "bare column reference should be gone: {out}"
        );
    }

    #[test]
    fn test_where_string_ne() {
        let fields = map(&[("level", true, false, false)]);
        let out = rewrite_sql("SELECT * FROM logs WHERE level != 'DEBUG'", &fields).unwrap();
        assert!(out.contains("level_str"), "{out}");
    }

    #[test]
    fn test_where_reversed_string_eq() {
        // `'ERROR' = level` — column is on the right side.
        let fields = map(&[("level", true, false, false)]);
        let out = rewrite_sql("SELECT * FROM logs WHERE 'ERROR' = level", &fields).unwrap();
        assert!(out.contains("level_str"), "{out}");
    }

    // --- Rule 3: WHERE numeric comparison ---

    #[test]
    fn test_where_numeric_gt() {
        let fields = map(&[("status", false, true, false)]);
        let out = rewrite_sql("SELECT * FROM logs WHERE status > 400", &fields).unwrap();
        assert!(out.contains("status_int"), "{out}");
    }

    #[test]
    fn test_where_numeric_lte() {
        let fields = map(&[("status", false, true, false)]);
        let out = rewrite_sql("SELECT * FROM logs WHERE status <= 200", &fields).unwrap();
        assert!(out.contains("status_int"), "{out}");
    }

    #[test]
    fn test_where_numeric_falls_back_to_float() {
        // No _int variant available — should fall back to _float.
        let fields = map(&[("latency", false, false, true)]);
        let out = rewrite_sql("SELECT * FROM logs WHERE latency > 1.5", &fields).unwrap();
        assert!(out.contains("latency_float"), "{out}");
    }

    // --- Rule 2+3 combined via AND ---

    #[test]
    fn test_where_and_chain() {
        let fields = map(&[
            ("level", true, false, false),
            ("status", false, true, false),
        ]);
        let out = rewrite_sql(
            "SELECT * FROM logs WHERE level = 'ERROR' AND status > 400",
            &fields,
        )
        .unwrap();
        assert!(out.contains("level_str"), "{out}");
        assert!(out.contains("status_int"), "{out}");
    }

    // --- IN list ---

    #[test]
    fn test_where_in_strings() {
        let fields = map(&[("level", true, false, false)]);
        let out = rewrite_sql(
            "SELECT * FROM logs WHERE level IN ('ERROR', 'WARN')",
            &fields,
        )
        .unwrap();
        assert!(out.contains("level_str"), "{out}");
    }

    // --- Rule 1: bare column in SELECT ---

    #[test]
    fn test_select_bare_str_only() {
        let fields = map(&[("level", true, false, false)]);
        let out = rewrite_sql("SELECT level FROM logs", &fields).unwrap();
        // str-only field → just x_str AS x
        assert!(out.contains("level_str"), "{out}");
        assert!(
            out.contains("AS level") || out.contains("level_str AS level"),
            "{out}"
        );
    }

    #[test]
    fn test_select_bare_int_and_str() {
        let fields = map(&[("status", true, true, false)]);
        let out = rewrite_sql("SELECT status FROM logs", &fields).unwrap();
        // int+str → COALESCE(CAST(status_int AS VARCHAR), status_str) AS status
        assert!(
            out.contains("COALESCE") || out.contains("coalesce"),
            "{out}"
        );
        assert!(out.contains("status_int"), "{out}");
        assert!(out.contains("status_str"), "{out}");
    }

    #[test]
    fn test_select_bare_int_only() {
        let fields = map(&[("count", false, true, false)]);
        let out = rewrite_sql("SELECT count FROM logs", &fields).unwrap();
        // int-only → CAST(count_int AS VARCHAR) AS count
        assert!(out.contains("count_int"), "{out}");
        assert!(out.contains("CAST") || out.contains("cast"), "{out}");
    }

    #[test]
    fn test_select_star_untouched() {
        let fields = map(&[("level", true, false, false)]);
        let out = rewrite_sql("SELECT * FROM logs", &fields).unwrap();
        // SELECT * should not be modified.
        assert!(out.contains('*'), "{out}");
    }

    // --- Rule 4: int(x) ---

    #[test]
    fn test_int_udf_rewrite_int_and_str() {
        let fields = map(&[("duration_ms", true, true, false)]);
        let out = rewrite_sql("SELECT int(duration_ms) AS d FROM logs", &fields).unwrap();
        // → COALESCE(duration_ms_int, TRY_CAST(duration_ms_str AS BIGINT)) AS d
        assert!(
            out.contains("COALESCE") || out.contains("coalesce"),
            "{out}"
        );
        assert!(out.contains("duration_ms_int"), "{out}");
        assert!(out.contains("duration_ms_str"), "{out}");
        assert!(
            out.to_uppercase().contains("BIGINT"),
            "expected BIGINT in: {out}"
        );
    }

    #[test]
    fn test_int_udf_rewrite_int_only() {
        let fields = map(&[("duration_ms", false, true, false)]);
        let out = rewrite_sql("SELECT int(duration_ms) AS d FROM logs", &fields).unwrap();
        // int-only: just duration_ms_int (no TRY_CAST needed)
        assert!(out.contains("duration_ms_int"), "{out}");
    }

    #[test]
    fn test_int_udf_rewrite_str_only() {
        let fields = map(&[("duration_ms", true, false, false)]);
        let out = rewrite_sql("SELECT int(duration_ms) AS d FROM logs", &fields).unwrap();
        // str-only: TRY_CAST(duration_ms_str AS BIGINT)
        assert!(out.contains("duration_ms_str"), "{out}");
        assert!(
            out.to_uppercase().contains("BIGINT"),
            "expected BIGINT in: {out}"
        );
    }

    // --- Unknown fields pass through ---

    #[test]
    fn test_unknown_field_passthrough() {
        let fields = map(&[("level", true, false, false)]);
        // `unknown_col` is not in FieldTypeMap — must not be modified.
        let sql = "SELECT unknown_col FROM logs WHERE unknown_col = 'foo'";
        let out = rewrite_sql(sql, &fields).unwrap();
        assert!(out.contains("unknown_col"), "{out}");
        assert!(!out.contains("unknown_col_str"), "{out}");
    }

    // --- Parse error propagation ---

    #[test]
    fn test_parse_error() {
        let fields = FieldTypeMap::new();
        let result = rewrite_sql("NOT VALID SQL @@@@", &fields);
        assert!(result.is_err());
    }
}
