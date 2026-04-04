// transform.rs — DataFusion SQL transform layer.
//
// Takes a user's SQL string, analyzes it at startup, compiles a DataFusion
// execution plan, and executes it against Arrow RecordBatches from the scanner.

use std::any::Any;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use datafusion::datasource::MemTable;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::*;

use logfwd_core::scan_config::ScanConfig;

pub use logfwd_arrow::conflict_schema;
pub mod udf;

// Re-export sqlparser through datafusion.
use datafusion::sql::sqlparser::ast::{
    self as sqlast, Expr as SqlExpr, SelectItem, SetExpr, Statement, WildcardAdditionalOptions,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

// ---------------------------------------------------------------------------
// QueryAnalyzer
// ---------------------------------------------------------------------------

/// Parses SQL at startup, extracts column references and determines scan config.
pub struct QueryAnalyzer {
    pub user_sql: String,
    pub referenced_columns: HashSet<String>,
    pub uses_select_star: bool,
    pub except_fields: Vec<String>,
    /// The WHERE clause AST, if present. Used for predicate pushdown extraction.
    where_clause: Option<SqlExpr>,
}

impl QueryAnalyzer {
    /// Parse the SQL and extract metadata about column usage.
    pub fn new(sql: &str) -> Result<Self, String> {
        let dialect = GenericDialect {};
        let statements =
            Parser::parse_sql(&dialect, sql).map_err(|e| format!("SQL parse error: {e}"))?;

        if statements.len() != 1 {
            return Err("Expected exactly one SQL statement".to_string());
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
            }
        } else {
            return Err("Only SELECT statements are supported".to_string());
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
                keep_raw: true,
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
                keep_raw: false,
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
    pub fn filter_hints(&self) -> logfwd_io::filter_hints::FilterHints {
        let mut hints = logfwd_io::filter_hints::FilterHints::default();

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

/// Walk a WHERE clause AST and extract predicates that can be pushed down.
/// Only extracts from top-level AND chains (not OR branches).
fn extract_pushable_predicates(expr: &SqlExpr, hints: &mut logfwd_io::filter_hints::FilterHints) {
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
        SqlExpr::Function(func) => match &func.args {
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
        },
        SqlExpr::Nested(inner) => {
            collect_column_refs(inner, cols);
        }
        SqlExpr::IsNull(e) | SqlExpr::IsNotNull(e) => {
            collect_column_refs(e, cols);
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
        // Literals, wildcards, etc. — no column refs.
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Custom UDFs: int() and float()
// ---------------------------------------------------------------------------

/// UDF: int(col) — safe cast from Utf8 to Int64, returns NULL on failure.
#[derive(Debug)]
pub(crate) struct IntCastUdf {
    signature: Signature,
}

impl IntCastUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IntCastUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "int"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(array) => {
                // Safe cast: returns NULL on parse failure.
                let result = arrow::compute::cast(array, &DataType::Int64)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                // Convert scalar to single-element array, cast, convert back.
                let array = scalar
                    .to_array()
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let result = arrow::compute::cast(&array, &DataType::Int64)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let scalar_val = datafusion::common::ScalarValue::try_from_array(&result, 0)?;
                Ok(ColumnarValue::Scalar(scalar_val))
            }
        }
    }
}

/// UDF: float(col) — safe cast from Utf8 to Float64, returns NULL on failure.
#[derive(Debug)]
struct FloatCastUdf {
    signature: Signature,
}

impl FloatCastUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FloatCastUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "float"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Array(array) => {
                let result = arrow::compute::cast(array, &DataType::Float64)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar
                    .to_array()
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let result = arrow::compute::cast(&array, &DataType::Float64)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let scalar_val = datafusion::common::ScalarValue::try_from_array(&result, 0)?;
                Ok(ColumnarValue::Scalar(scalar_val))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SqlTransform
// ---------------------------------------------------------------------------

/// Manages a DataFusion context, compiles and caches plans, executes SQL
/// transforms against Arrow RecordBatches.
///
/// The SessionContext and UDFs are created once and reused across batches.
/// The `logs` MemTable and enrichment tables are swapped per batch
/// (deregister + register). This eliminates the per-batch cost of
/// SessionContext construction, built-in function registration, and
/// UDF compilation.
pub struct SqlTransform {
    user_sql: String,
    analyzer: QueryAnalyzer,
    /// Schema fingerprint for cache invalidation.
    schema_hash: u64,
    /// Enrichment tables registered alongside `logs` in each DataFusion session.
    enrichment_tables: Vec<Arc<dyn logfwd_io::enrichment::EnrichmentTable>>,
    /// Optional geo-IP database for the `geo_lookup()` UDF.
    geo_database: Option<Arc<dyn logfwd_io::enrichment::GeoDatabase>>,
    /// Cached DataFusion session — created once, table swapped per batch.
    ctx: Option<SessionContext>,
}

impl SqlTransform {
    /// Create a new SQL transform from a SQL string.
    pub fn new(sql: &str) -> Result<Self, String> {
        let analyzer = QueryAnalyzer::new(sql)?;

        Ok(SqlTransform {
            user_sql: sql.to_string(),
            analyzer,
            schema_hash: 0,
            enrichment_tables: Vec::new(),
            geo_database: None,
            ctx: None,
        })
    }

    /// Set the geo-IP database for the `geo_lookup()` UDF.
    ///
    /// Invalidates the cached SessionContext so the UDF is re-registered
    /// with the new database on the next execute() call.
    pub fn set_geo_database(&mut self, db: Arc<dyn logfwd_io::enrichment::GeoDatabase>) {
        self.geo_database = Some(db);
        self.ctx = None; // force re-creation with new geo UDF
    }

    /// Add an enrichment table that will be registered in each DataFusion
    /// session alongside the `logs` table. Returns an error if a table with
    /// the same name is already registered or if the name conflicts with "logs".
    ///
    /// Does NOT invalidate the cached SessionContext (unlike `set_geo_database`)
    /// because enrichment tables are deregistered/re-registered per batch in
    /// `execute()`. The context doesn't need to know about them at creation time.
    pub fn add_enrichment_table(
        &mut self,
        table: Arc<dyn logfwd_io::enrichment::EnrichmentTable>,
    ) -> Result<(), String> {
        let name = table.name();
        if name == "logs" {
            return Err("enrichment table cannot be named 'logs' (reserved)".to_string());
        }
        if self.enrichment_tables.iter().any(|t| t.name() == name) {
            return Err(format!("duplicate enrichment table name: '{name}'"));
        }
        self.enrichment_tables.push(table);
        Ok(())
    }

    /// Execute the SQL transform on a RecordBatch.
    ///
    /// Reuses a cached DataFusion SessionContext across batches. The `logs`
    /// MemTable is swapped per batch (deregister + register). UDFs and
    /// built-in functions persist across batches.
    ///
    /// Schema changes (new fields in later batches) are handled automatically
    /// since the MemTable is recreated with the batch's schema each call.
    pub async fn execute(&mut self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.num_rows() == 0 {
            return Ok(batch);
        }

        // Invalidate the cached SessionContext when the schema changes.
        //
        // DataFusion caches logical plans inside the SessionContext. If the
        // batch schema changes between calls (new fields, type conflicts resolved
        // differently), the cached plan refers to a stale schema and execution
        // will fail or produce incorrect results. Forcing ctx = None causes
        // ensure_context() to build a fresh SessionContext with no stale plans.
        let new_hash = hash_schema(batch.schema());
        if new_hash != self.schema_hash {
            self.ctx = None;
        }
        self.schema_hash = new_hash;

        // Ensure the SessionContext exists (created once, reused across batches).
        self.ensure_context();
        let ctx = self.ctx.as_ref().expect("context just ensured");

        // Swap the `logs` table: build new table first, then deregister + register.
        // Building the MemTable before deregistering ensures that on error we
        // don't leave the context without a `logs` table.
        //
        // Normalize the batch first: if the scanner detected type conflicts it
        // emits struct columns (`status: Struct { int: Int64, str: Utf8View }`).
        // Replace each conflict struct with a flat `status: Utf8` column so SQL
        // using bare names resolves on both clean and conflict batches.
        let batch = conflict_schema::normalize_conflict_columns(batch);
        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| format!("Failed to create MemTable: {e}"))?;
        let _ = ctx.deregister_table("logs");
        ctx.register_table("logs", Arc::new(table))
            .map_err(|e| format!("Failed to register table: {e}"))?;

        // Swap enrichment tables whose snapshots have changed.
        // If snapshot() returns None (table not loaded yet), deregister the
        // stale table — queries referencing it will fail with a clear error
        // rather than silently returning stale data.
        for et in &self.enrichment_tables {
            let _ = ctx.deregister_table(et.name());
            if let Some(snapshot) = et.snapshot() {
                let et_table =
                    MemTable::try_new(snapshot.schema(), vec![vec![snapshot]]).map_err(|e| {
                        format!("Failed to create enrichment table '{}': {e}", et.name())
                    })?;
                ctx.register_table(et.name(), Arc::new(et_table))
                    .map_err(|e| {
                        format!("Failed to register enrichment table '{}': {e}", et.name())
                    })?;
            } else {
                eprintln!(
                    "  warning: enrichment table '{}' not yet loaded, skipping",
                    et.name()
                );
            }
        }

        // Execute the SQL.
        let sql = &self.user_sql;
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| format!("SQL execution error: {e}"))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Failed to collect results: {e}"))?;

        // Concat all result batches into one.
        match batches.len() {
            0 => {
                let df2 = ctx
                    .sql(sql)
                    .await
                    .map_err(|e| format!("SQL schema error: {e}"))?;
                let df_schema = df2.schema();
                Ok(RecordBatch::new_empty(Arc::clone(df_schema.inner())))
            }
            1 => Ok(batches.into_iter().next().expect("verified len==1")),
            _ => {
                let schema = batches[0].schema();
                concat_batches(&schema, &batches)
                    .map_err(|e| format!("Failed to concat batches: {e}"))
            }
        }
    }

    /// Lazily create the SessionContext with UDFs registered.
    ///
    /// Created lazily (not in new()) because set_geo_database() and
    /// add_enrichment_table() may be called after construction.
    fn ensure_context(&mut self) {
        if self.ctx.is_some() {
            return;
        }
        let ctx = SessionContext::new();

        // Register custom UDFs once — they persist across batches.
        ctx.register_udf(ScalarUDF::from(IntCastUdf::new()));
        ctx.register_udf(ScalarUDF::from(FloatCastUdf::new()));
        ctx.register_udf(ScalarUDF::from(udf::RegexpExtractUdf::new()));
        ctx.register_udf(ScalarUDF::from(udf::GrokUdf::new()));
        ctx.register_udf(ScalarUDF::from(udf::JsonExtractUdf::new(
            udf::JsonExtractMode::Str,
        )));
        ctx.register_udf(ScalarUDF::from(udf::JsonExtractUdf::new(
            udf::JsonExtractMode::Int,
        )));
        ctx.register_udf(ScalarUDF::from(udf::JsonExtractUdf::new(
            udf::JsonExtractMode::Float,
        )));
        if let Some(ref db) = self.geo_database {
            ctx.register_udf(ScalarUDF::from(udf::geo_lookup::GeoLookupUdf::new(
                Arc::clone(db),
            )));
        }

        self.ctx = Some(ctx);
    }

    /// Synchronous wrapper around [`execute`](Self::execute) for callers that
    /// are not yet async. When called from within a tokio runtime, uses
    /// `block_in_place` + the current handle. Otherwise creates a temporary
    /// runtime.
    ///
    /// When the calling code is made async, switch to `execute().await` directly.
    pub fn execute_blocking(&mut self, batch: RecordBatch) -> Result<RecordBatch, String> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(self.execute(batch))),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;
                rt.block_on(self.execute(batch))
            }
        }
    }

    /// Get the ScanConfig for field pushdown.
    pub fn scan_config(&self) -> ScanConfig {
        self.analyzer.scan_config()
    }

    /// Get a reference to the query analyzer.
    pub fn analyzer(&self) -> &QueryAnalyzer {
        &self.analyzer
    }

    /// Validate the SQL plan by executing it against a dummy single-row batch.
    ///
    /// This forces DataFusion to plan the query (resolve columns, check for
    /// duplicate aliases, validate window specs, etc.) at validation time
    /// rather than waiting for the first real batch at runtime.
    pub fn validate_plan(&mut self) -> Result<(), String> {
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::{Field, Schema};

        // Build a schema from referenced columns (or a fallback for SELECT *).
        let fields: Vec<Field> = if self.analyzer.referenced_columns.is_empty() {
            // SELECT * — provide a minimal representative schema.
            vec![
                Field::new("_raw", DataType::Utf8, true),
                Field::new("level", DataType::Utf8, true),
                Field::new("msg", DataType::Utf8, true),
            ]
        } else {
            self.analyzer
                .referenced_columns
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect()
        };

        let schema = Arc::new(Schema::new(fields.clone()));
        let arrays: Vec<ArrayRef> = fields
            .iter()
            .map(|_| Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef)
            .collect();

        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| format!("failed to build probe batch: {e}"))?;

        self.execute_blocking(batch)?;
        Ok(())
    }
}

/// Concatenate multiple RecordBatches into one.
fn concat_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<RecordBatch, arrow::error::ArrowError> {
    arrow::compute::concat_batches(schema, batches)
}

/// Hash an Arrow schema for quick change detection.
fn hash_schema(schema: SchemaRef) -> u64 {
    let mut hasher = DefaultHasher::new();
    for field in schema.fields() {
        field.name().hash(&mut hasher);
        field.data_type().hash(&mut hasher);
        field.is_nullable().hash(&mut hasher);
    }
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};

    /// Helper: build a simple test RecordBatch with bare-name columns matching
    /// what the Phase-10 scanner emits for single-type fields.
    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("msg", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
        ]));
        let level: ArrayRef = Arc::new(StringArray::from(vec![
            Some("INFO"),
            Some("ERROR"),
            Some("DEBUG"),
            Some("ERROR"),
        ]));
        let msg: ArrayRef = Arc::new(StringArray::from(vec![
            Some("started"),
            Some("disk full"),
            Some("heartbeat"),
            Some("oom killed"),
        ]));
        let status: ArrayRef = Arc::new(StringArray::from(vec![
            Some("200"),
            Some("500"),
            Some("not_a_number"),
            Some("503"),
        ]));
        RecordBatch::try_new(schema, vec![level, msg, status]).unwrap()
    }

    #[test]
    fn test_simple_passthrough() {
        let batch = make_test_batch();
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        let result = transform.execute_blocking(batch.clone()).unwrap();
        assert_eq!(result.num_rows(), 4);
        assert_eq!(result.num_columns(), 3);
        // Verify data matches.
        let level = result
            .column_by_name("level")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(level.value(0), "INFO");
        assert_eq!(level.value(3), "ERROR");
    }

    #[test]
    fn test_filter() {
        let batch = make_test_batch();
        let mut transform = SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        let level = result
            .column_by_name("level")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..result.num_rows() {
            assert_eq!(level.value(i), "ERROR");
        }
        let msg = result
            .column_by_name("msg")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(msg.value(0), "disk full");
        assert_eq!(msg.value(1), "oom killed");
    }

    #[test]
    fn test_except() {
        let batch = make_test_batch();
        let mut transform = SqlTransform::new("SELECT * EXCEPT (status) FROM logs").unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 4);
        // status should be removed.
        assert!(result.column_by_name("status").is_none());
        // Other columns should remain.
        assert!(result.column_by_name("level").is_some());
        assert!(result.column_by_name("msg").is_some());
    }

    #[test]
    fn test_computed() {
        let batch = make_test_batch();
        let mut transform = SqlTransform::new("SELECT *, 'prod' AS env FROM logs").unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 4);
        let env = result
            .column_by_name("env")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..4 {
            assert_eq!(env.value(i), "prod");
        }
    }

    #[test]
    fn test_int_udf() {
        let batch = make_test_batch();
        let mut transform =
            SqlTransform::new("SELECT int(status) AS status_int FROM logs").unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 4);
        let status = result
            .column_by_name("status_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(status.value(0), 200);
        assert_eq!(status.value(1), 500);
        assert!(status.is_null(2)); // "not_a_number" should be NULL
        assert_eq!(status.value(3), 503);
    }

    #[test]
    fn test_schema_evolution() {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

        // First batch: 2 columns.
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
        ]));
        let batch1 = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(StringArray::from(vec!["web1", "web2"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["INFO", "ERROR"])) as ArrayRef,
            ],
        )
        .unwrap();

        let result1 = transform.execute_blocking(batch1).unwrap();
        assert_eq!(result1.num_columns(), 2);
        assert_eq!(result1.num_rows(), 2);

        // Second batch: 3 columns (new field added).
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
        ]));
        let batch2 = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(StringArray::from(vec!["web3"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["WARN"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["us-east-1"])) as ArrayRef,
            ],
        )
        .unwrap();

        let result2 = transform.execute_blocking(batch2).unwrap();
        assert_eq!(result2.num_columns(), 3);
        assert_eq!(result2.num_rows(), 1);
        assert!(result2.column_by_name("region").is_some());
    }

    #[test]
    fn test_float_udf() {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, true)]));
        let vals: ArrayRef = Arc::new(StringArray::from(vec![
            Some("3.25"),
            Some("not_float"),
            Some("2.125"),
        ]));
        let batch = RecordBatch::try_new(schema, vec![vals]).unwrap();

        let mut transform = SqlTransform::new("SELECT float(val) AS val_f FROM logs").unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        let col = result
            .column_by_name("val_f")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 3.25).abs() < 1e-10);
        assert!(col.is_null(1));
        assert!((col.value(2) - 2.125).abs() < 1e-10);
    }

    #[test]
    fn test_query_analyzer_column_refs() {
        let analyzer =
            QueryAnalyzer::new("SELECT level, msg FROM logs WHERE status = '500'").unwrap();
        assert!(!analyzer.uses_select_star);
        assert!(analyzer.referenced_columns.contains("level"));
        assert!(analyzer.referenced_columns.contains("msg"));
        assert!(analyzer.referenced_columns.contains("status"));
    }

    #[test]
    fn test_query_analyzer_select_star() {
        let analyzer = QueryAnalyzer::new("SELECT * FROM logs").unwrap();
        assert!(analyzer.uses_select_star);
        assert!(analyzer.except_fields.is_empty());
    }

    #[test]
    fn test_query_analyzer_except() {
        let analyzer = QueryAnalyzer::new("SELECT * EXCEPT (stack_trace) FROM logs").unwrap();
        assert!(analyzer.uses_select_star);
        assert_eq!(analyzer.except_fields, vec!["stack_trace"]);
    }

    #[test]
    fn test_enrichment_cross_join() {
        use logfwd_io::enrichment::StaticTable;

        let batch = make_test_batch();
        let mut transform =
            SqlTransform::new("SELECT logs.*, env.environment FROM logs CROSS JOIN env").unwrap();

        // Add a static enrichment table.
        let env_table = Arc::new(
            StaticTable::new(
                "env",
                &[("environment".to_string(), "production".to_string())],
            )
            .expect("valid labels"),
        );
        transform.add_enrichment_table(env_table).unwrap();

        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 4);

        // Should have original columns plus "environment".
        let env_col = result
            .column_by_name("environment")
            .expect("should have environment column")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..4 {
            assert_eq!(env_col.value(i), "production");
        }
    }

    #[test]
    fn test_enrichment_unused_table_no_error() {
        use logfwd_io::enrichment::StaticTable;

        let batch = make_test_batch();
        let table = Arc::new(
            StaticTable::new("unused", &[("key".to_string(), "val".to_string())])
                .expect("valid labels"),
        );

        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        transform.add_enrichment_table(table).unwrap();

        // Enrichment table registered but not referenced in SQL — should not error.
        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_enrichment_empty_table_skipped() {
        use logfwd_io::enrichment::K8sPathTable;

        let batch = make_test_batch();
        let k8s = Arc::new(K8sPathTable::new("k8s_pods"));
        // Not loaded — snapshot() returns None.

        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        transform.add_enrichment_table(k8s).unwrap();

        // Should not error — empty table just skipped.
        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    // --- FilterHints predicate pushdown tests ---

    #[test]
    fn test_filter_hints_severity_lte() {
        let a = QueryAnalyzer::new("SELECT * FROM logs WHERE severity <= 4").unwrap();
        let h = a.filter_hints();
        assert_eq!(h.max_severity, Some(4));
    }

    #[test]
    fn test_filter_hints_severity_lt() {
        let a = QueryAnalyzer::new("SELECT * FROM logs WHERE severity < 5").unwrap();
        let h = a.filter_hints();
        assert_eq!(h.max_severity, Some(4)); // < 5 becomes <= 4
    }

    #[test]
    fn test_filter_hints_severity_eq() {
        let a = QueryAnalyzer::new("SELECT * FROM logs WHERE severity = 3").unwrap();
        let h = a.filter_hints();
        assert_eq!(h.max_severity, Some(3));
    }

    #[test]
    fn test_filter_hints_facility_eq() {
        let a = QueryAnalyzer::new("SELECT * FROM logs WHERE facility = 16").unwrap();
        let h = a.filter_hints();
        assert_eq!(h.facilities, Some(vec![16]));
    }

    #[test]
    fn test_filter_hints_facility_in() {
        let a = QueryAnalyzer::new("SELECT * FROM logs WHERE facility IN (1, 4, 16)").unwrap();
        let h = a.filter_hints();
        let mut facs = h.facilities.unwrap();
        facs.sort_unstable();
        assert_eq!(facs, vec![1, 4, 16]);
    }

    #[test]
    fn test_filter_hints_combined_and() {
        let a =
            QueryAnalyzer::new("SELECT * FROM logs WHERE severity <= 4 AND facility = 16").unwrap();
        let h = a.filter_hints();
        assert_eq!(h.max_severity, Some(4));
        assert_eq!(h.facilities, Some(vec![16]));
    }

    #[test]
    fn test_filter_hints_or_not_pushed() {
        // OR with non-pushable predicate — should NOT push severity
        let a =
            QueryAnalyzer::new("SELECT * FROM logs WHERE severity <= 4 OR msg = 'error'").unwrap();
        let h = a.filter_hints();
        // The OR is at top level, not an AND chain — nothing should be pushed
        assert!(h.max_severity.is_none());
    }

    #[test]
    fn test_filter_hints_no_where() {
        let a = QueryAnalyzer::new("SELECT * FROM logs").unwrap();
        let h = a.filter_hints();
        assert!(h.max_severity.is_none());
        assert!(h.facilities.is_none());
        assert!(h.wanted_fields.is_none()); // SELECT * = all fields
    }

    #[test]
    fn test_filter_hints_field_pushdown() {
        let a =
            QueryAnalyzer::new("SELECT hostname, message FROM logs WHERE severity <= 2").unwrap();
        let h = a.filter_hints();
        assert_eq!(h.max_severity, Some(2));
        let mut fields = h.wanted_fields.unwrap();
        fields.sort();
        assert!(fields.contains(&"hostname".to_string()));
        assert!(fields.contains(&"message".to_string()));
        assert!(fields.contains(&"severity".to_string())); // referenced in WHERE
    }

    #[test]
    fn test_filter_hints_typed_column_stripped() {
        // With the struct conflict format there are no more `__int`/`__str` suffixed
        // columns. `strip_type_suffix` is a no-op. `severity__int` is an unrecognised
        // column name, so no severity pushdown fires.
        let a = QueryAnalyzer::new("SELECT * FROM logs WHERE severity__int <= 4").unwrap();
        let h = a.filter_hints();
        assert_eq!(h.max_severity, None);
    }

    #[test]
    fn test_filter_hints_or_pushable_only_still_not_pushed() {
        // OR of only pushable predicates still blocks pushdown
        let a =
            QueryAnalyzer::new("SELECT * FROM logs WHERE severity <= 4 OR severity <= 2").unwrap();
        let h = a.filter_hints();
        assert!(h.max_severity.is_none());
    }

    #[test]
    fn test_filter_hints_reversed_gt() {
        // "5 > severity" means severity < 5, i.e. severity <= 4
        let a = QueryAnalyzer::new("SELECT * FROM logs WHERE 5 > severity").unwrap();
        let h = a.filter_hints();
        assert_eq!(h.max_severity, Some(4));
    }

    #[test]
    fn test_filter_hints_tighten_severity() {
        // Multiple severity bounds should keep the tightest (minimum)
        let a =
            QueryAnalyzer::new("SELECT * FROM logs WHERE severity <= 4 AND severity <= 2").unwrap();
        let h = a.filter_hints();
        assert_eq!(h.max_severity, Some(2)); // tighter bound wins
    }

    #[test]
    fn test_filter_hints_tighten_facility() {
        // Multiple facility constraints should intersect
        let a =
            QueryAnalyzer::new("SELECT * FROM logs WHERE facility IN (1, 4, 16) AND facility = 4")
                .unwrap();
        let h = a.filter_hints();
        assert_eq!(h.facilities, Some(vec![4])); // intersection
    }

    // -----------------------------------------------------------------------
    // Multi-batch context caching tests
    // -----------------------------------------------------------------------

    /// Verify that cached SessionContext doesn't leak data between batches.
    /// Batch 1 and batch 2 have different data — a WHERE filter on batch 2
    /// should never return rows from batch 1.
    #[test]
    fn test_cached_context_no_data_leakage() {
        let mut transform = SqlTransform::new("SELECT host FROM logs WHERE host = 'web2'").unwrap();

        // Batch 1: only web1.
        let batch1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["web1", "web1"])) as ArrayRef],
        )
        .unwrap();

        let result1 = transform.execute_blocking(batch1).unwrap();
        assert_eq!(result1.num_rows(), 0, "batch 1 should match nothing");

        // Batch 2: has web2.
        let batch2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["web2", "web3"])) as ArrayRef],
        )
        .unwrap();

        let result2 = transform.execute_blocking(batch2).unwrap();
        assert_eq!(result2.num_rows(), 1, "batch 2 should match one row");

        // Batch 3: no web2 again — verify old data from batch 2 is gone.
        let batch3 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["web4"])) as ArrayRef],
        )
        .unwrap();

        let result3 = transform.execute_blocking(batch3).unwrap();
        assert_eq!(
            result3.num_rows(),
            0,
            "batch 3 should not contain leftover data from batch 2"
        );
    }

    /// Verify that many consecutive batches on the same SqlTransform work correctly.
    /// This exercises the deregister/register cycle repeatedly.
    #[test]
    fn test_cached_context_many_batches() {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));

        for i in 0..20 {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![i as i64])) as ArrayRef],
            )
            .unwrap();

            let result = transform.execute_blocking(batch).unwrap();
            assert_eq!(result.num_rows(), 1);
            let col = result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(col.value(0), i as i64, "batch {i} has wrong value");
        }
    }

    // -----------------------------------------------------------------------
    // CASE expression tests
    // -----------------------------------------------------------------------

    /// Verify that collect_column_refs correctly extracts column names from a
    /// CASE expression so that scan_config() requests the right fields.
    ///
    /// This is a regression test for the bug where CASE conditions and results
    /// fell through to the catch-all arm and were silently ignored, causing the
    /// scanner to never extract those fields and DataFusion to fail with
    /// "column not found" errors at execution time.
    #[test]
    fn test_query_analyzer_case_column_refs() {
        let sql = "SELECT CASE \
                       WHEN level = 'ERROR' THEN 'high' \
                       WHEN level = 'WARN'  THEN 'medium' \
                       ELSE 'low' \
                   END AS severity FROM logs";
        let a = QueryAnalyzer::new(sql).unwrap();
        // `level` must be collected so scan_config requests it from the scanner.
        assert!(
            a.referenced_columns.contains("level"),
            "expected 'level' in referenced_columns, got {:?}",
            a.referenced_columns
        );
    }

    /// Verify that a CASE expression with column references in both WHEN
    /// conditions and THEN results executes correctly end-to-end.
    #[test]
    fn test_case_expression_in_select() {
        let batch = make_test_batch();
        let mut transform = SqlTransform::new(
            "SELECT CASE \
                 WHEN level = 'ERROR' THEN 'high' \
                 WHEN level = 'WARN'  THEN 'medium' \
                 ELSE 'low' \
             END AS severity FROM logs",
        )
        .unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 4);

        let severity = result
            .column_by_name("severity")
            .expect("severity column must be present")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Row 0: level=INFO → 'low'
        assert_eq!(severity.value(0), "low");
        // Row 1: level=ERROR → 'high'
        assert_eq!(severity.value(1), "high");
        // Row 2: level=DEBUG → 'low'
        assert_eq!(severity.value(2), "low");
        // Row 3: level=ERROR → 'high'
        assert_eq!(severity.value(3), "high");
    }

    /// Verify that a searched CASE expression used in a WHERE clause executes
    /// correctly end-to-end.  Column references inside the CASE conditions must
    /// be resolved by DataFusion (i.e. the scanner must have been asked to
    /// extract those columns via scan_config).
    #[test]
    fn test_case_expression_in_where() {
        let batch = make_test_batch();
        // Keep only rows where the CASE evaluates to 'high' (i.e. level=ERROR).
        let mut transform = SqlTransform::new(
            "SELECT msg FROM logs WHERE \
             CASE WHEN level = 'ERROR' THEN 'high' ELSE 'low' END = 'high'",
        )
        .unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 2);

        let msg = result
            .column_by_name("msg")
            .expect("msg column must be present")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(msg.value(0), "disk full");
        assert_eq!(msg.value(1), "oom killed");
    }

    /// Verify that collect_column_refs picks up column references from CASE
    /// THEN results (not just WHEN conditions).
    #[test]
    fn test_query_analyzer_case_result_column_refs() {
        // The THEN clause references `msg` — it must appear in referenced_columns.
        let sql = "SELECT CASE WHEN level = 'ERROR' THEN msg ELSE 'ok' END AS out FROM logs";
        let a = QueryAnalyzer::new(sql).unwrap();
        assert!(
            a.referenced_columns.contains("level"),
            "expected 'level' in referenced_columns, got {:?}",
            a.referenced_columns
        );
        assert!(
            a.referenced_columns.contains("msg"),
            "expected 'msg' in referenced_columns, got {:?}",
            a.referenced_columns
        );
    }

    /// Verify that a CASE expression whose THEN clause yields a column value
    /// executes correctly end-to-end.
    #[test]
    fn test_case_expression_result_is_column() {
        let batch = make_test_batch();
        // For ERROR rows return msg, otherwise return a literal.
        let mut transform = SqlTransform::new(
            "SELECT CASE WHEN level = 'ERROR' THEN msg ELSE 'ok' END AS out FROM logs",
        )
        .unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        assert_eq!(result.num_rows(), 4);

        let out = result
            .column_by_name("out")
            .expect("out column must be present")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Row 0: INFO → 'ok'
        assert_eq!(out.value(0), "ok");
        // Row 1: ERROR → msg = 'disk full'
        assert_eq!(out.value(1), "disk full");
        // Row 2: DEBUG → 'ok'
        assert_eq!(out.value(2), "ok");
        // Row 3: ERROR → msg = 'oom killed'
        assert_eq!(out.value(3), "oom killed");
    }

    // -----------------------------------------------------------------------
    // Schema-change invalidation tests
    // -----------------------------------------------------------------------

    /// Regression test: schema change across batches must invalidate the cached
    /// SessionContext so that the new batch's schema is planned correctly.
    ///
    /// Before the fix, `schema_hash` was tracked but never used to clear
    /// `self.ctx`, so the old plan referencing the previous schema was reused,
    /// causing "column not found" errors or silent wrong results when new fields
    /// appeared.
    #[test]
    fn test_schema_change_new_field_invalidates_cache() {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

        // Batch 1: two columns.
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("msg", DataType::Utf8, true),
        ]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema1),
            vec![
                Arc::new(StringArray::from(vec!["INFO"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
            ],
        )
        .unwrap();
        let r1 = transform.execute_blocking(batch1).unwrap();
        assert_eq!(r1.num_rows(), 1);
        assert_eq!(r1.num_columns(), 2);

        // Batch 2: three columns (added "host").  Without the fix, the stale
        // SessionContext plan would fail or miss the new column.
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("msg", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
        ]));
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema2),
            vec![
                Arc::new(StringArray::from(vec!["ERROR"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["disk full"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["web1"])) as ArrayRef,
            ],
        )
        .unwrap();
        let r2 = transform.execute_blocking(batch2).unwrap();
        assert_eq!(r2.num_rows(), 1);
        // All three columns must be present after schema change.
        assert_eq!(
            r2.num_columns(),
            3,
            "expected 3 columns after schema change"
        );
        let host = r2
            .column_by_name("host")
            .expect("'host' column must be present after schema change")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(host.value(0), "web1");
    }

    /// Schema changes across batches where column types change (e.g., a field
    /// that was Int64 becomes Utf8) must also re-plan correctly.
    #[test]
    fn test_schema_change_type_conflict_invalidates_cache() {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

        // Batch 1: "status" is Int64.
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
        ]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema1),
            vec![
                Arc::new(StringArray::from(vec!["INFO"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![200i64])) as ArrayRef,
            ],
        )
        .unwrap();
        let r1 = transform.execute_blocking(batch1).unwrap();
        assert_eq!(r1.num_rows(), 1);

        // Batch 2: "status" is now Utf8 (type conflict resolved differently).
        // Without the fix, DataFusion would reuse the Int64 plan against a Utf8
        // column and fail.
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
        ]));
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema2),
            vec![
                Arc::new(StringArray::from(vec!["ERROR"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["not_a_number"])) as ArrayRef,
            ],
        )
        .unwrap();
        let r2 = transform.execute_blocking(batch2).unwrap();
        assert_eq!(r2.num_rows(), 1);
        let status = r2
            .column_by_name("status")
            .expect("'status' column must be present")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(status.value(0), "not_a_number");
    }

    // -----------------------------------------------------------------------
    // Regression tests — #415: IS NULL / IS NOT NULL / LIKE / numeric IN list
    // -----------------------------------------------------------------------

    /// `WHERE status IS NULL` on a plain Utf8 batch.
    /// collect_column_refs must collect "status" from an IsNull expression so
    /// the scanner requests the field, and DataFusion must filter correctly.
    #[test]
    fn test_where_is_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            true,
        )]));
        let status: ArrayRef = Arc::new(StringArray::from(vec![Some("200"), None, Some("404")]));
        let batch = RecordBatch::try_new(schema, vec![status]).unwrap();

        let mut transform =
            SqlTransform::new("SELECT status FROM logs WHERE status IS NULL").unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        // Only the null row should match.
        assert_eq!(result.num_rows(), 1);
    }

    /// `WHERE status IS NOT NULL` on a plain Utf8 batch.
    #[test]
    fn test_where_is_not_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            true,
        )]));
        let status: ArrayRef = Arc::new(StringArray::from(vec![Some("200"), None, Some("404")]));
        let batch = RecordBatch::try_new(schema, vec![status]).unwrap();

        let mut transform =
            SqlTransform::new("SELECT status FROM logs WHERE status IS NOT NULL").unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        // The two non-null rows should match.
        assert_eq!(result.num_rows(), 2);
    }

    /// `WHERE level LIKE 'ERR%'` on a plain batch.
    /// collect_column_refs must collect "level" from a Like expression.
    #[test]
    fn test_where_like() {
        let batch = make_test_batch();
        let mut transform =
            SqlTransform::new("SELECT * FROM logs WHERE level LIKE 'ERR%'").unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        // make_test_batch has two ERROR rows.
        assert_eq!(result.num_rows(), 2);
    }

    /// `WHERE int(status) IN (200, 503)` on a plain batch.
    /// collect_column_refs must collect "status" from the InList expression's
    /// function argument.
    #[test]
    fn test_where_int_in_list() {
        let batch = make_test_batch();
        let mut transform =
            SqlTransform::new("SELECT * FROM logs WHERE int(status) IN (200, 503)").unwrap();
        let result = transform.execute_blocking(batch).unwrap();
        // make_test_batch has status values "200", "500", "not_a_number", "503"
        // int(status) IN (200, 503) matches rows 0 and 3.
        assert_eq!(result.num_rows(), 2);
    }

    // -----------------------------------------------------------------------
    // Regression tests — QueryAnalyzer column-ref collection for #415 forms
    // -----------------------------------------------------------------------

    /// QueryAnalyzer must collect column refs from an IS NULL expression.
    #[test]
    fn test_query_analyzer_is_null_column_refs() {
        let a = QueryAnalyzer::new("SELECT level FROM logs WHERE status IS NULL").unwrap();
        assert!(
            a.referenced_columns.contains("status"),
            "IS NULL expr must add 'status' to referenced_columns"
        );
        assert!(
            a.referenced_columns.contains("level"),
            "'level' from SELECT must be in referenced_columns"
        );
    }

    /// QueryAnalyzer must collect column refs from a LIKE expression.
    #[test]
    fn test_query_analyzer_like_column_refs() {
        let a = QueryAnalyzer::new("SELECT * FROM logs WHERE level LIKE 'ERR%'").unwrap();
        assert!(
            a.referenced_columns.contains("level"),
            "LIKE expr must add 'level' to referenced_columns"
        );
    }

    /// QueryAnalyzer must collect column refs from an IN list expression.
    #[test]
    fn test_query_analyzer_in_list_column_refs() {
        let a = QueryAnalyzer::new("SELECT * FROM logs WHERE status IN ('200', '404')").unwrap();
        assert!(
            a.referenced_columns.contains("status"),
            "InList expr must add 'status' to referenced_columns"
        );
    }

    // -----------------------------------------------------------------------

    /// Verify that a stable schema does NOT trigger repeated context recreation
    /// (i.e. the hash comparison is correct and equal hashes are treated as
    /// cache hits).
    #[test]
    fn test_stable_schema_does_not_invalidate_cache() {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("msg", DataType::Utf8, true),
        ]));

        // Run 5 batches with the same schema; each must succeed and the context
        // must still be populated (not None) after each run.
        for i in 0u32..5 {
            let val = format!("row{i}");
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec![val.as_str()])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["msg"])) as ArrayRef,
                ],
            )
            .unwrap();
            let r = transform.execute_blocking(batch).unwrap();
            assert_eq!(r.num_rows(), 1, "batch {i} must return 1 row");
            // The context must still be alive (not wiped by a false hash mismatch).
            assert!(
                transform.ctx.is_some(),
                "ctx must remain populated for stable schema (batch {i})"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Bug #721: validate_plan catches SQL planning errors
    // -----------------------------------------------------------------------

    #[test]
    fn validate_plan_accepts_valid_sql() {
        let mut transform = SqlTransform::new("SELECT * FROM logs").unwrap();
        transform
            .validate_plan()
            .expect("valid SQL should pass plan validation");
    }

    #[test]
    fn validate_plan_catches_duplicate_aliases() {
        // "SELECT level AS k, msg AS k FROM logs" produces a duplicate alias error
        // at DataFusion planning time (not at parse time).
        let mut transform = SqlTransform::new("SELECT level AS k, msg AS k FROM logs").unwrap();
        let err = transform.validate_plan();
        assert!(
            err.is_err(),
            "duplicate column alias should be caught by validate_plan"
        );
    }

    #[test]
    fn validate_plan_catches_invalid_function() {
        // A non-existent function should fail during planning.
        let mut transform = SqlTransform::new("SELECT nonexistent_fn(level) FROM logs").unwrap();
        let err = transform.validate_plan();
        assert!(
            err.is_err(),
            "unknown function should be caught by validate_plan"
        );
    }

    #[test]
    fn validate_plan_accepts_filter_query() {
        let mut transform = SqlTransform::new("SELECT * FROM logs WHERE level = 'ERROR'").unwrap();
        transform
            .validate_plan()
            .expect("filter query should pass plan validation");
    }
}
