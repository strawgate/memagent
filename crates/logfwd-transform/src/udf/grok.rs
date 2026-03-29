//! UDF: grok(string, pattern) -> Struct
//!
//! Logstash-style grok pattern extraction. Expands grok patterns like
//! `%{PATTERN:name}` into named regex capture groups, then returns a Struct
//! with one field per named capture.
//!
//! ```sql
//! -- Extract structured fields from access logs
//! SELECT grok(message_str, '%{WORD:method} %{URIPATH:path} %{NUMBER:status}')
//! FROM logs
//!
//! -- Access individual fields
//! SELECT grok(message_str, '%{IP:client} %{NUMBER:duration}').client AS client_ip
//! FROM logs
//!
//! -- Compose with int()/float() for type conversion
//! SELECT int(grok(message_str, '%{WORD:method} %{URIPATH:path} %{NUMBER:status}').status) AS code
//! FROM logs
//! ```
//!
//! Built-in patterns: IP, IPV4, IPV6, NUMBER, INT, BASE10NUM, WORD, NOTSPACE,
//! SPACE, DATA, GREEDYDATA, QUOTEDSTRING, UUID, MAC, URIPATH, URIPATHPARAM,
//! URI, TIMESTAMP_ISO8601, DATE, TIME, LOGLEVEL, HOSTNAME, EMAILADDRESS.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, StringBuilder, StructArray};
use arrow::datatypes::{DataType, Field, Fields};

use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use regex::Regex;

// ---------------------------------------------------------------------------
// Built-in grok patterns
// ---------------------------------------------------------------------------

/// Core grok patterns. These mirror the Logstash/Elastic defaults.
fn builtin_patterns() -> HashMap<&'static str, &'static str> {
    let mut m = HashMap::new();

    // Network
    m.insert("IPV4", r"\b(?:\d{1,3}\.){3}\d{1,3}\b");
    m.insert(
        "IPV6",
        r"\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b|\b(?:[0-9a-fA-F]{1,4}:){1,7}:|:(?::[0-9a-fA-F]{1,4}){1,7}\b",
    );
    m.insert(
        "IP",
        r"(?:\b(?:\d{1,3}\.){3}\d{1,3}\b|\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b)",
    );
    m.insert("MAC", r"\b[0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5}\b");
    m.insert("HOSTNAME", r"\b[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*\b");

    // Numbers
    m.insert("INT", r"[+-]?\d+");
    m.insert("NUMBER", r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?");
    m.insert("BASE10NUM", r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?");
    m.insert("BASE16NUM", r"0[xX][0-9a-fA-F]+");

    // Text
    m.insert("WORD", r"\b\w+\b");
    m.insert("NOTSPACE", r"\S+");
    m.insert("SPACE", r"\s+");
    m.insert("DATA", r".*?");
    m.insert("GREEDYDATA", r".*");
    m.insert("QUOTEDSTRING", r#""(?:[^"\\]|\\.)*""#);

    // URIs
    m.insert("URIPATH", r"/[^\s?#]*");
    m.insert("URIPATHPARAM", r"/[^\s]*");
    m.insert("URI", r"\S+://\S+");

    // Identifiers
    m.insert(
        "UUID",
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
    );
    m.insert(
        "EMAILADDRESS",
        r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b",
    );

    // Timestamps
    m.insert(
        "TIMESTAMP_ISO8601",
        r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?",
    );
    m.insert("DATE", r"\d{4}-\d{2}-\d{2}");
    m.insert("TIME", r"\d{2}:\d{2}:\d{2}(?:\.\d+)?");

    // Logging
    m.insert(
        "LOGLEVEL",
        r"\b(?:TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL)\b",
    );
    m.insert(
        "HTTPMETHOD",
        r"\b(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|CONNECT|TRACE)\b",
    );
    m.insert("STATUSCODE", r"\b[1-5]\d{2}\b");

    m
}

// ---------------------------------------------------------------------------
// Grok pattern compiler
// ---------------------------------------------------------------------------

/// A compiled grok pattern: the regex and the ordered list of capture group names.
#[derive(Debug)]
struct CompiledGrok {
    regex: Regex,
    field_names: Vec<String>,
}

/// Expand `%{PATTERN:name}` and `%{PATTERN}` references into a regex with
/// named capture groups. Returns the compiled regex and the list of field names.
fn compile_grok(pattern: &str) -> Result<CompiledGrok, String> {
    let patterns = builtin_patterns();
    let mut regex_str = String::with_capacity(pattern.len() * 2);
    let mut field_names = Vec::new();
    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut token = String::new();
            for ch in chars.by_ref() {
                if ch == '}' {
                    break;
                }
                token.push(ch);
            }
            // Parse PATTERN:name or just PATTERN
            let (pat_name, capture_name) = if let Some(colon_pos) = token.find(':') {
                (&token[..colon_pos], Some(&token[colon_pos + 1..]))
            } else {
                (token.as_str(), None)
            };

            let pat_regex = patterns
                .get(pat_name)
                .ok_or_else(|| format!("unknown grok pattern: {pat_name}"))?;

            if let Some(name) = capture_name {
                regex_str.push_str(&format!("(?P<{name}>{pat_regex})"));
                field_names.push(name.to_string());
            } else {
                // Unnamed: wrap in non-capturing group
                regex_str.push_str(&format!("(?:{pat_regex})"));
            }
        } else {
            // Escape regex metacharacters in literal parts
            match c {
                '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' => {
                    regex_str.push('\\');
                    regex_str.push(c);
                }
                '\\' => {
                    // Pass through escape sequences
                    regex_str.push('\\');
                    if let Some(&next) = chars.peek() {
                        regex_str.push(next);
                        chars.next();
                    }
                }
                _ => regex_str.push(c),
            }
        }
    }

    let regex = Regex::new(&regex_str)
        .map_err(|e| format!("grok pattern compiled to invalid regex: {e}"))?;

    Ok(CompiledGrok { regex, field_names })
}

// ---------------------------------------------------------------------------
// GrokUdf
// ---------------------------------------------------------------------------

/// UDF: grok(string, pattern) -> Struct<field1: Utf8, field2: Utf8, ...>
///
/// The pattern uses Logstash-style `%{PATTERN:name}` syntax. The return type
/// is a Struct with one Utf8 field per named capture group.
#[derive(Debug)]
pub struct GrokUdf {
    signature: Signature,
}

impl Default for GrokUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl GrokUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GrokUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "grok"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        // We can't know the struct fields without seeing the pattern.
        // Return a placeholder — DataFusion calls return_type_from_args for actual planning.
        // For now return Utf8 as fallback; the real work is in return_type_from_args.
        Ok(DataType::Utf8)
    }

    fn return_type_from_args(
        &self,
        args: datafusion::logical_expr::ReturnTypeArgs,
    ) -> DfResult<datafusion::logical_expr::ReturnInfo> {
        // If the pattern argument is a literal, extract field names and return Struct type.
        if args.scalar_arguments.len() >= 2
            && let Some(datafusion::common::ScalarValue::Utf8(Some(pattern_str))) =
                args.scalar_arguments[1]
            && let Ok(compiled) = compile_grok(pattern_str)
        {
            let fields: Vec<Field> = compiled
                .field_names
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect();
            if !fields.is_empty() {
                return Ok(datafusion::logical_expr::ReturnInfo::new_nullable(
                    DataType::Struct(Fields::from(fields)),
                ));
            }
        }
        // Fallback: can't determine struct fields, return Utf8
        Ok(datafusion::logical_expr::ReturnInfo::new_nullable(
            DataType::Utf8,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let input = &args.args[0];
        let pattern = &args.args[1];

        // Extract pattern string.
        let pattern_str = match pattern {
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(s) => {
                let s = s.to_string();
                s.trim_matches('"').trim_matches('\'').to_string()
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_string::<i32>();
                if str_arr.len() == 0 || str_arr.is_null(0) {
                    return Ok(ColumnarValue::Scalar(
                        datafusion::common::ScalarValue::Utf8(None),
                    ));
                }
                str_arr.value(0).to_string()
            }
        };

        // Compile grok pattern.
        let compiled = compile_grok(&pattern_str)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("grok: {e}")))?;

        match input {
            ColumnarValue::Array(array) => {
                let str_array = array.as_string::<i32>();
                let num_rows = str_array.len();

                // Build one StringBuilder per capture group.
                let mut builders: Vec<StringBuilder> = compiled
                    .field_names
                    .iter()
                    .map(|_| StringBuilder::with_capacity(num_rows, num_rows * 32))
                    .collect();

                for row in 0..num_rows {
                    if str_array.is_null(row) {
                        for b in &mut builders {
                            b.append_null();
                        }
                        continue;
                    }
                    let val = str_array.value(row);
                    match compiled.regex.captures(val) {
                        Some(caps) => {
                            for (i, name) in compiled.field_names.iter().enumerate() {
                                match caps.name(name) {
                                    Some(m) => builders[i].append_value(m.as_str()),
                                    None => builders[i].append_null(),
                                }
                            }
                        }
                        None => {
                            for b in &mut builders {
                                b.append_null();
                            }
                        }
                    }
                }

                // Build the struct array.
                let fields: Vec<Field> = compiled
                    .field_names
                    .iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect();
                let arrays: Vec<ArrayRef> = builders
                    .into_iter()
                    .map(|mut b| Arc::new(b.finish()) as ArrayRef)
                    .collect();

                let struct_array = StructArray::new(Fields::from(fields), arrays, None);
                Ok(ColumnarValue::Array(Arc::new(struct_array)))
            }
            ColumnarValue::Scalar(scalar) => {
                let val = scalar.to_string();
                let val = val.trim_matches('"').trim_matches('\'');

                let fields: Vec<Field> = compiled
                    .field_names
                    .iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect();

                match compiled.regex.captures(val) {
                    Some(caps) => {
                        let values: Vec<datafusion::common::ScalarValue> = compiled
                            .field_names
                            .iter()
                            .map(|name| match caps.name(name) {
                                Some(m) => datafusion::common::ScalarValue::Utf8(Some(
                                    m.as_str().to_string(),
                                )),
                                None => datafusion::common::ScalarValue::Utf8(None),
                            })
                            .collect();
                        Ok(ColumnarValue::Scalar(
                            datafusion::common::ScalarValue::Struct(Arc::new(StructArray::from(
                                fields
                                    .into_iter()
                                    .zip(values)
                                    .map(|(f, v)| (Arc::new(f), v.to_array().unwrap() as ArrayRef))
                                    .collect::<Vec<_>>(),
                            ))),
                        ))
                    }
                    None => {
                        // Return struct with all NULL fields
                        let values: Vec<datafusion::common::ScalarValue> = compiled
                            .field_names
                            .iter()
                            .map(|_| datafusion::common::ScalarValue::Utf8(None))
                            .collect();
                        Ok(ColumnarValue::Scalar(
                            datafusion::common::ScalarValue::Struct(Arc::new(StructArray::from(
                                fields
                                    .into_iter()
                                    .zip(values)
                                    .map(|(f, v)| (Arc::new(f), v.to_array().unwrap() as ArrayRef))
                                    .collect::<Vec<_>>(),
                            ))),
                        ))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, StringArray};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::*;

    async fn run_sql(batch: RecordBatch, sql: &str) -> RecordBatch {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GrokUdf::new()));
        // Also register int() for composition tests
        ctx.register_udf(ScalarUDF::from(crate::udf::RegexpExtractUdf::new()));
        let table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("logs", Arc::new(table)).unwrap();
        let df = ctx.sql(sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        batches.into_iter().next().unwrap()
    }

    fn make_access_log_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let msgs: ArrayRef = Arc::new(StringArray::from(vec![
            Some("GET /api/users 200 15ms"),
            Some("POST /api/orders 500 230ms"),
            Some("no match here"),
            None,
        ]));
        arrow::record_batch::RecordBatch::try_new(schema, vec![msgs]).unwrap()
    }

    #[test]
    fn test_compile_grok_basic() {
        let compiled = compile_grok("%{WORD:method} %{URIPATH:path} %{NUMBER:status}").unwrap();
        assert_eq!(compiled.field_names, vec!["method", "path", "status"]);
        assert!(compiled.regex.is_match("GET /api/users 200"));
    }

    #[test]
    fn test_compile_grok_unnamed() {
        let compiled = compile_grok("%{WORD} %{NUMBER:code}").unwrap();
        assert_eq!(compiled.field_names, vec!["code"]);
    }

    #[test]
    fn test_compile_grok_unknown_pattern() {
        let result = compile_grok("%{DOESNOTEXIST:foo}");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown grok pattern"));
    }

    #[test]
    fn test_grok_struct_access() {
        let batch = make_access_log_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT grok(message, '%{WORD:method} %{URIPATH:path} %{NUMBER:status} %{NUMBER:duration}ms') AS parsed FROM logs",
        ));

        // Result should have a struct column "parsed"
        assert_eq!(result.num_rows(), 4);
        let parsed = result.column_by_name("parsed").unwrap();
        let struct_arr = parsed.as_struct();

        let method = struct_arr
            .column_by_name("method")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(method.value(0), "GET");
        assert_eq!(method.value(1), "POST");
        assert!(method.is_null(2)); // no match
        assert!(method.is_null(3)); // NULL input

        let status = struct_arr
            .column_by_name("status")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(status.value(0), "200");
        assert_eq!(status.value(1), "500");
    }

    #[test]
    fn test_grok_dot_notation() {
        let batch = make_access_log_batch();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT get_field(grok(message, '%{WORD:method} %{URIPATH:path} %{NUMBER:status} %{NUMBER:duration}ms'), 'method') AS http_method FROM logs",
        ));

        let method = result
            .column_by_name("http_method")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(method.value(0), "GET");
        assert_eq!(method.value(1), "POST");
    }

    #[test]
    fn test_grok_ip_pattern() {
        let schema = Arc::new(Schema::new(vec![Field::new("log", DataType::Utf8, true)]));
        let logs: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Connection from 192.168.1.100 port 22"),
            Some("Request from 10.0.0.1 port 443"),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![logs]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            "SELECT get_field(grok(log, '%{GREEDYDATA} from %{IPV4:ip} port %{INT:port}'), 'ip') AS client_ip FROM logs",
        ));

        let ip = result
            .column_by_name("client_ip")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ip.value(0), "192.168.1.100");
        assert_eq!(ip.value(1), "10.0.0.1");
    }
}
