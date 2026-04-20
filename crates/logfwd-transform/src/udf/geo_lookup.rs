//! UDF: geo_lookup(ip: Utf8|Utf8View|LargeUtf8) -> Struct{country_code, country_name, city, region, latitude, longitude, asn, org}
//!
//! Enriches log records with geographic location data based on IP addresses.
//! Wraps a pluggable [`GeoDatabase`] backend — use [`MmdbDatabase`] for
//! MaxMind GeoLite2-City / GeoIP2-City (`.mmdb` format).
//!
//! ```sql
//! -- Full geo struct
//! SELECT *, geo_lookup(client_ip_str) AS geo FROM logs
//!
//! -- Access individual fields
//! SELECT geo_lookup(client_ip_str).country_code AS country FROM logs
//!
//! -- Filter on country
//! SELECT * FROM logs WHERE geo_lookup(client_ip_str).country_code != 'US'
//! ```
//!
//! Returns NULL fields for unresolvable IPs (private ranges, malformed, not in DB).

use std::any::Any;
use std::net::IpAddr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Float64Builder, Int64Builder, StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, Fields};

use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

use crate::enrichment::{GeoDatabase, GeoResult};

// ---------------------------------------------------------------------------
// Schema helper
// ---------------------------------------------------------------------------

/// Fixed Arrow schema returned by geo_lookup().
fn geo_result_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("country_code", DataType::Utf8, true),
        Field::new("country_name", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
        Field::new("asn", DataType::Int64, true),
        Field::new("org", DataType::Utf8, true),
    ]))
}

// ---------------------------------------------------------------------------
// GeoLookupUdf
// ---------------------------------------------------------------------------

/// UDF: geo_lookup(ip: Utf8|Utf8View|LargeUtf8) -> Struct{country_code, country_name, city, region, latitude, longitude, asn, org}
///
/// Calls the underlying [`GeoDatabase`] for each row. Returns a struct with all
/// NULL fields for IPs that cannot be resolved (private ranges, malformed, absent
/// from the database).
pub struct GeoLookupUdf {
    signature: Signature,
    db: Arc<dyn GeoDatabase>,
}

impl std::fmt::Debug for GeoLookupUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeoLookupUdf").finish_non_exhaustive()
    }
}

impl PartialEq for GeoLookupUdf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature && Arc::ptr_eq(&self.db, &other.db)
    }
}

impl Eq for GeoLookupUdf {}

impl std::hash::Hash for GeoLookupUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        Arc::as_ptr(&self.db).hash(state);
    }
}

impl GeoLookupUdf {
    /// Create a new UDF wrapping the given database backend.
    pub fn new(db: Arc<dyn GeoDatabase>) -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                ]),
                Volatility::Immutable,
            ),
            db,
        }
    }
}

impl ScalarUDFImpl for GeoLookupUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "geo_lookup"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(geo_result_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "geo_lookup() expects exactly one argument".to_string(),
            ));
        }

        let input = &args.args[0];

        match input {
            ColumnarValue::Array(array) => {
                let num_rows = array.len();

                // One builder per struct field.
                let mut country_code = StringBuilder::with_capacity(num_rows, num_rows * 4);
                let mut country_name = StringBuilder::with_capacity(num_rows, num_rows * 16);
                let mut city = StringBuilder::with_capacity(num_rows, num_rows * 16);
                let mut region = StringBuilder::with_capacity(num_rows, num_rows * 16);
                let mut latitude = Float64Builder::with_capacity(num_rows);
                let mut longitude = Float64Builder::with_capacity(num_rows);
                let mut asn = Int64Builder::with_capacity(num_rows);
                let mut org = StringBuilder::with_capacity(num_rows, num_rows * 16);

                match array.data_type() {
                    DataType::Utf8 => {
                        let strings = array.as_string::<i32>();
                        for row in 0..num_rows {
                            let result = if strings.is_null(row) {
                                None
                            } else {
                                self.db.lookup(strings.value(row))
                            };
                            append_result(
                                result.as_ref(),
                                &mut country_code,
                                &mut country_name,
                                &mut city,
                                &mut region,
                                &mut latitude,
                                &mut longitude,
                                &mut asn,
                                &mut org,
                            );
                        }
                    }
                    DataType::Utf8View => {
                        let strings = array.as_string_view();
                        for row in 0..num_rows {
                            let result = if strings.is_null(row) {
                                None
                            } else {
                                self.db.lookup(strings.value(row))
                            };
                            append_result(
                                result.as_ref(),
                                &mut country_code,
                                &mut country_name,
                                &mut city,
                                &mut region,
                                &mut latitude,
                                &mut longitude,
                                &mut asn,
                                &mut org,
                            );
                        }
                    }
                    DataType::LargeUtf8 => {
                        let strings = array.as_string::<i64>();
                        for row in 0..num_rows {
                            let result = if strings.is_null(row) {
                                None
                            } else {
                                self.db.lookup(strings.value(row))
                            };
                            append_result(
                                result.as_ref(),
                                &mut country_code,
                                &mut country_name,
                                &mut city,
                                &mut region,
                                &mut latitude,
                                &mut longitude,
                                &mut asn,
                                &mut org,
                            );
                        }
                    }
                    other => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "geo_lookup() input must be Utf8/Utf8View/LargeUtf8, got {other:?}"
                        )));
                    }
                }

                let struct_array = build_struct_array(
                    country_code,
                    country_name,
                    city,
                    region,
                    latitude,
                    longitude,
                    asn,
                    org,
                );
                Ok(ColumnarValue::Array(Arc::new(struct_array)))
            }

            ColumnarValue::Scalar(scalar) => {
                // Single-value path: look up once, return a scalar struct.
                let ip_str = match scalar {
                    datafusion::common::ScalarValue::Utf8(Some(s))
                    | datafusion::common::ScalarValue::Utf8View(Some(s))
                    | datafusion::common::ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
                    _ => None,
                };
                let result = ip_str.and_then(|ip| self.db.lookup(ip));
                let scalar_val = geo_result_to_scalar(result.as_ref())?;
                Ok(ColumnarValue::Scalar(scalar_val))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Append one row (possibly NULL) to all builders.
#[allow(clippy::too_many_arguments)]
fn append_result(
    result: Option<&GeoResult>,
    country_code: &mut StringBuilder,
    country_name: &mut StringBuilder,
    city: &mut StringBuilder,
    region: &mut StringBuilder,
    latitude: &mut Float64Builder,
    longitude: &mut Float64Builder,
    asn: &mut Int64Builder,
    org: &mut StringBuilder,
) {
    match result {
        Some(r) => {
            match &r.country_code {
                Some(v) => country_code.append_value(v),
                None => country_code.append_null(),
            }
            match &r.country_name {
                Some(v) => country_name.append_value(v),
                None => country_name.append_null(),
            }
            match &r.city {
                Some(v) => city.append_value(v),
                None => city.append_null(),
            }
            match &r.region {
                Some(v) => region.append_value(v),
                None => region.append_null(),
            }
            match r.latitude {
                Some(v) => latitude.append_value(v),
                None => latitude.append_null(),
            }
            match r.longitude {
                Some(v) => longitude.append_value(v),
                None => longitude.append_null(),
            }
            match r.asn {
                Some(v) => asn.append_value(v),
                None => asn.append_null(),
            }
            match &r.org {
                Some(v) => org.append_value(v),
                None => org.append_null(),
            }
        }
        None => {
            country_code.append_null();
            country_name.append_null();
            city.append_null();
            region.append_null();
            latitude.append_null();
            longitude.append_null();
            asn.append_null();
            org.append_null();
        }
    }
}

/// Assemble a StructArray from finished builders.
#[allow(clippy::too_many_arguments)]
fn build_struct_array(
    mut country_code: StringBuilder,
    mut country_name: StringBuilder,
    mut city: StringBuilder,
    mut region: StringBuilder,
    mut latitude: Float64Builder,
    mut longitude: Float64Builder,
    mut asn: Int64Builder,
    mut org: StringBuilder,
) -> StructArray {
    let DataType::Struct(fields) = geo_result_type() else {
        unreachable!()
    };
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(country_code.finish()),
        Arc::new(country_name.finish()),
        Arc::new(city.finish()),
        Arc::new(region.finish()),
        Arc::new(latitude.finish()),
        Arc::new(longitude.finish()),
        Arc::new(asn.finish()),
        Arc::new(org.finish()),
    ];
    StructArray::new(fields, arrays, None)
}

/// Convert a single `GeoResult` into a DataFusion `ScalarValue::Struct`.
fn geo_result_to_scalar(result: Option<&GeoResult>) -> DfResult<datafusion::common::ScalarValue> {
    let DataType::Struct(fields) = geo_result_type() else {
        unreachable!()
    };

    let utf8 = |v: Option<&String>| datafusion::common::ScalarValue::Utf8(v.cloned());
    let f64_val = |v: Option<f64>| datafusion::common::ScalarValue::Float64(v);
    let i64_val = |v: Option<i64>| datafusion::common::ScalarValue::Int64(v);

    let values: Vec<datafusion::common::ScalarValue> = match result {
        Some(r) => vec![
            utf8(r.country_code.as_ref()),
            utf8(r.country_name.as_ref()),
            utf8(r.city.as_ref()),
            utf8(r.region.as_ref()),
            f64_val(r.latitude),
            f64_val(r.longitude),
            i64_val(r.asn),
            utf8(r.org.as_ref()),
        ],
        None => vec![
            datafusion::common::ScalarValue::Utf8(None),
            datafusion::common::ScalarValue::Utf8(None),
            datafusion::common::ScalarValue::Utf8(None),
            datafusion::common::ScalarValue::Utf8(None),
            datafusion::common::ScalarValue::Float64(None),
            datafusion::common::ScalarValue::Float64(None),
            datafusion::common::ScalarValue::Int64(None),
            datafusion::common::ScalarValue::Utf8(None),
        ],
    };

    let pairs: Vec<(Arc<Field>, ArrayRef)> = fields
        .iter()
        .zip(values.iter())
        .map(|(f, v)| Ok((Arc::clone(f), v.to_array()?)))
        .collect::<DfResult<_>>()?;

    Ok(datafusion::common::ScalarValue::Struct(Arc::new(
        StructArray::from(pairs),
    )))
}

// ---------------------------------------------------------------------------
// MMDB backend (MaxMind GeoLite2-City / GeoIP2-City)
// ---------------------------------------------------------------------------

/// MaxMind MMDB database backend.
///
/// Supports `.mmdb` files in GeoLite2-City or GeoIP2-City format.
/// The database is loaded into memory once at construction time.
///
/// ```rust,no_run
/// use logfwd_transform::enrichment::GeoDatabase as _;
/// use logfwd_transform::udf::geo_lookup::MmdbDatabase;
///
/// let db = MmdbDatabase::open("/etc/logfwd/GeoLite2-City.mmdb").unwrap();
/// let result = db.lookup("8.8.8.8");
/// ```
pub struct MmdbDatabase {
    reader: maxminddb::Reader<Vec<u8>>,
}

impl std::fmt::Debug for MmdbDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmdbDatabase").finish()
    }
}

impl MmdbDatabase {
    /// Open an MMDB file at the given path and load it into memory.
    ///
    /// Returns an error if the file cannot be read or is not a valid MMDB.
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::TransformError> {
        let data = std::fs::read(path.as_ref()).map_err(|e| {
            crate::TransformError::Enrichment(format!(
                "failed to read MMDB file '{}': {e}",
                path.as_ref().display()
            ))
        })?;
        let reader = maxminddb::Reader::from_source(data).map_err(|e| {
            crate::TransformError::Enrichment(format!(
                "failed to parse MMDB file '{}': {e}",
                path.as_ref().display()
            ))
        })?;
        Ok(Self { reader })
    }
}

impl GeoDatabase for MmdbDatabase {
    fn lookup(&self, ip: &str) -> Option<GeoResult> {
        let addr: IpAddr = ip.parse().ok()?;
        let lookup = self.reader.lookup(addr).ok()?;
        if !lookup.has_data() {
            return None;
        }
        let city: maxminddb::geoip2::City<'_> = lookup.decode().ok()??;

        let country_code = city.country.iso_code.map(str::to_owned);

        let country_name = city.country.names.english.map(str::to_owned);

        let city_name = city.city.names.english.map(str::to_owned);

        let region = city
            .subdivisions
            .first()
            .and_then(|sub| sub.names.english)
            .map(str::to_owned);

        let latitude = city.location.latitude;
        let longitude = city.location.longitude;

        // ASN and org are not present in City databases; they live in the
        // separate GeoLite2-ASN database. Return None for both.
        Some(GeoResult {
            country_code,
            country_name,
            city: city_name,
            region,
            latitude,
            longitude,
            asn: None,
            org: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::*;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // Mock GeoDatabase for unit tests (no real MMDB file needed)
    // -----------------------------------------------------------------------

    struct MockGeoDatabase {
        entries: HashMap<String, GeoResult>,
    }

    impl MockGeoDatabase {
        fn new(entries: impl IntoIterator<Item = (&'static str, GeoResult)>) -> Self {
            Self {
                entries: entries
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
            }
        }
    }

    impl GeoDatabase for MockGeoDatabase {
        fn lookup(&self, ip: &str) -> Option<GeoResult> {
            self.entries.get(ip).cloned()
        }
    }

    fn us_result() -> GeoResult {
        GeoResult {
            country_code: Some("US".to_string()),
            country_name: Some("United States".to_string()),
            city: Some("Seattle".to_string()),
            region: Some("Washington".to_string()),
            latitude: Some(47.6062),
            longitude: Some(-122.3321),
            asn: Some(16509),
            org: Some("Amazon.com".to_string()),
        }
    }

    fn de_result() -> GeoResult {
        GeoResult {
            country_code: Some("DE".to_string()),
            country_name: Some("Germany".to_string()),
            city: None,
            region: None,
            latitude: Some(51.1657),
            longitude: Some(10.4515),
            asn: None,
            org: None,
        }
    }

    fn make_db() -> Arc<dyn GeoDatabase> {
        Arc::new(MockGeoDatabase::new([
            ("1.2.3.4", us_result()),
            ("5.6.7.8", de_result()),
        ]))
    }

    async fn run_sql(batch: RecordBatch, db: Arc<dyn GeoDatabase>, sql: &str) -> RecordBatch {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(GeoLookupUdf::new(db)));
        let table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("logs", Arc::new(table)).unwrap();
        let df = ctx.sql(sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        batches.into_iter().next().unwrap()
    }

    fn make_batch(ips: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("ip", DataType::Utf8, true)]));
        let arr: ArrayRef = Arc::new(StringArray::from(ips));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    // -----------------------------------------------------------------------
    // Unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn geo_result_type_schema() {
        let dt = geo_result_type();
        let DataType::Struct(fields) = dt else {
            panic!("expected Struct");
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "country_code",
                "country_name",
                "city",
                "region",
                "latitude",
                "longitude",
                "asn",
                "org"
            ]
        );
    }

    #[test]
    fn geo_lookup_array_known_ip() {
        let db = make_db();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            make_batch(vec![Some("1.2.3.4"), Some("5.6.7.8")]),
            db,
            "SELECT geo_lookup(ip) AS geo FROM logs",
        ));

        assert_eq!(result.num_rows(), 2);
        let geo = result.column_by_name("geo").unwrap();
        let s = geo.as_struct();

        let cc = s.column_by_name("country_code").unwrap().as_string::<i32>();
        assert_eq!(cc.value(0), "US");
        assert_eq!(cc.value(1), "DE");

        let lat = s
            .column_by_name("latitude")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((lat.value(0) - 47.6062).abs() < 1e-4);
        assert!((lat.value(1) - 51.1657).abs() < 1e-4);

        // Row 0 has ASN, row 1 does not.
        let asn = s
            .column_by_name("asn")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(asn.value(0), 16509);
        assert!(asn.is_null(1));
    }

    #[test]
    fn geo_lookup_unresolvable_ip_returns_null_fields() {
        let db = make_db();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            make_batch(vec![Some("192.168.1.1")]),
            db,
            "SELECT geo_lookup(ip) AS geo FROM logs",
        ));

        let geo = result.column_by_name("geo").unwrap();
        let s = geo.as_struct();
        let cc = s.column_by_name("country_code").unwrap().as_string::<i32>();
        assert!(cc.is_null(0));
        let lat = s
            .column_by_name("latitude")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(lat.is_null(0));
    }

    #[test]
    fn geo_lookup_null_input_returns_null_fields() {
        let db = make_db();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            make_batch(vec![None]),
            db,
            "SELECT geo_lookup(ip) AS geo FROM logs",
        ));

        let geo = result.column_by_name("geo").unwrap();
        let s = geo.as_struct();
        let cc = s.column_by_name("country_code").unwrap().as_string::<i32>();
        assert!(cc.is_null(0));
    }

    #[test]
    fn geo_lookup_field_access() {
        let db = make_db();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Use get_field() to avoid the nested-runtime panic that occurs when
        // DataFusion evaluates struct dot-notation in a test-owned runtime.
        // (Same pattern as grok.rs tests; dot notation works in production via
        // execute_blocking which creates its own fresh runtime.)
        let result = rt.block_on(run_sql(
            make_batch(vec![Some("1.2.3.4"), Some("5.6.7.8"), None]),
            db,
            "SELECT get_field(geo_lookup(ip), 'country_code') AS country FROM logs",
        ));

        let country = result
            .column_by_name("country")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(country.value(0), "US");
        assert_eq!(country.value(1), "DE");
        assert!(country.is_null(2));
    }

    #[test]
    fn geo_lookup_filter_by_country() {
        let db = make_db();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            make_batch(vec![Some("1.2.3.4"), Some("5.6.7.8")]),
            db,
            "SELECT ip FROM logs WHERE get_field(geo_lookup(ip), 'country_code') = 'DE'",
        ));

        assert_eq!(result.num_rows(), 1);
        let ip = result
            .column_by_name("ip")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ip.value(0), "5.6.7.8");
    }

    #[test]
    fn geo_lookup_malformed_ip_returns_null_fields() {
        let db = Arc::new(MockGeoDatabase::new([]));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            make_batch(vec![Some("not-an-ip"), Some("999.999.999.999")]),
            db,
            "SELECT geo_lookup(ip) AS geo FROM logs",
        ));

        let geo = result.column_by_name("geo").unwrap();
        let s = geo.as_struct();
        let cc = s.column_by_name("country_code").unwrap().as_string::<i32>();
        assert!(cc.is_null(0));
        assert!(cc.is_null(1));
    }

    /// Regression: geo_lookup() must accept Utf8View input columns.
    /// Before the fix, the signature only included Utf8 and the UDF would
    /// reject Utf8View arrays (e.g. from Parquet readers or certain transforms).
    #[test]
    fn geo_lookup_utf8view_input() {
        use arrow::array::StringViewArray;

        let db = make_db();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ip",
            DataType::Utf8View,
            true,
        )]));
        let arr: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("1.2.3.4"),
            Some("5.6.7.8"),
            None,
        ]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            db,
            "SELECT get_field(geo_lookup(ip), 'country_code') AS cc FROM logs",
        ));

        let cc = result
            .column_by_name("cc")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cc.value(0), "US");
        assert_eq!(cc.value(1), "DE");
        assert!(cc.is_null(2));
    }

    #[test]
    fn geo_lookup_largeutf8_input() {
        use arrow::array::LargeStringArray;

        let db = make_db();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ip",
            DataType::LargeUtf8,
            true,
        )]));
        let arr: ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some("1.2.3.4"),
            Some("5.6.7.8"),
            None,
        ]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(run_sql(
            batch,
            db,
            "SELECT get_field(geo_lookup(ip), 'country_code') AS cc FROM logs",
        ));

        let cc = result
            .column_by_name("cc")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cc.value(0), "US");
        assert_eq!(cc.value(1), "DE");
        assert!(cc.is_null(2));
    }

    #[test]
    fn geo_lookup_mixed_rows() {
        let db = make_db();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Mix: known, unknown, null, known.
        let result = rt.block_on(run_sql(
            make_batch(vec![
                Some("1.2.3.4"),
                Some("10.0.0.1"),
                None,
                Some("5.6.7.8"),
            ]),
            db,
            "SELECT get_field(geo_lookup(ip), 'country_code') AS cc FROM logs",
        ));

        let cc = result
            .column_by_name("cc")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cc.value(0), "US");
        assert!(cc.is_null(1)); // unknown
        assert!(cc.is_null(2)); // null input
        assert_eq!(cc.value(3), "DE");
    }
}
