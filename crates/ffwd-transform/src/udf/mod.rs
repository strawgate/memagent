//! Custom scalar UDFs for the ffwd SQL transform engine.
//!
//! These are registered in `SqlTransform::execute()` and available in user SQL.

mod bounded_lru;

/// CSV-backed IP-range geo database implementation and helpers.
pub mod csv_range_geo;
pub mod geo_lookup;
pub mod grok;
pub mod hash;
pub mod json_extract;
pub mod regexp_extract;

/// CSV IP-range geo database backend re-exported for runtime wiring.
pub use csv_range_geo::CsvRangeDatabase;
pub use geo_lookup::GeoLookupUdf;
pub use grok::GrokUdf;
pub use hash::HashUdf;
pub use json_extract::{JsonExtractMode, JsonExtractUdf};
pub use regexp_extract::RegexpExtractUdf;
