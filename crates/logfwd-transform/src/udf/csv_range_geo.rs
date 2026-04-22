//! CSV IP-range geo-IP backend.
//!
//! A drop-in replacement for [`MmdbDatabase`](crate::udf::geo_lookup::MmdbDatabase)
//! that reads a plain CSV file
//! instead of a MaxMind `.mmdb` binary.  Compatible with the free
//! [DB-IP Lite](https://db-ip.com/db/lite/ip-to-location) exports and any
//! similar CSV with `ip_range_start` / `ip_range_end` header columns.
//!
//! ## CSV column mapping
//!
//! | CSV header (case-insensitive) | [`GeoResult`](crate::enrichment::GeoResult) field |
//! |-------------------------------|---------------------|
//! | `ip_range_start` or `start_ip`| *(range key)*       |
//! | `ip_range_end`   or `end_ip`  | *(range key)*       |
//! | `country_code`                | `country_code`      |
//! | `country_name`                | `country_name`      |
//! | `city`                        | `city`              |
//! | `stateprov` or `region`       | `region`            |
//! | `latitude`                    | `latitude`          |
//! | `longitude`                   | `longitude`         |
//! | `asn`                         | `asn`               |
//! | `org` or `organization`       | `org`               |
//!
//! Unrecognised columns are silently ignored.
//!
//! ## Lookup algorithm
//!
//! Ranges are stored in a sorted `Vec` keyed on the start address as a `u128`
//! (IPv4 addresses use the IPv4-mapped IPv6 representation).  A binary search
//! locates the last entry whose start ≤ lookup IP; if its end ≥ lookup IP the
//! entry is a hit.
//!
//! ## Example (YAML config)
//!
//! ```yaml
//! enrichment:
//!   - type: geo_database
//!     format: csv_range
//!     path: /etc/logfwd/dbip-city-lite.csv
//! ```

use std::io;
use std::net::IpAddr;
use std::path::Path;
use std::str::FromStr;

use crate::TransformError;
use crate::enrichment::{GeoDatabase, GeoResult};

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct RangeEntry {
    start: u128,
    end: u128,
    result: GeoResult,
}

// ---------------------------------------------------------------------------
// CsvRangeDatabase
// ---------------------------------------------------------------------------

/// Geo-IP database backed by a CSV file of IP ranges.
///
/// See the [module-level documentation](self) for the expected CSV format.
#[derive(Debug)]
pub struct CsvRangeDatabase {
    ranges: Vec<RangeEntry>,
}

impl CsvRangeDatabase {
    /// Load from a CSV reader.
    pub fn load_from_reader<R: io::Read>(reader: R) -> Result<Self, TransformError> {
        let mut csv = csv::ReaderBuilder::new().flexible(true).from_reader(reader);

        let headers: Vec<String> = csv
            .headers()
            .map_err(|e| TransformError::Enrichment(format!("CSV header error: {e}")))?
            .iter()
            .map(|h| h.trim().to_lowercase())
            .collect();

        // Locate required and optional column indices.
        let start_idx = headers
            .iter()
            .position(|h| h == "ip_range_start" || h == "start_ip")
            .ok_or_else(|| {
                TransformError::Enrichment(
                    "CSV missing required column 'ip_range_start' or 'start_ip'".to_string(),
                )
            })?;

        let end_idx = headers
            .iter()
            .position(|h| h == "ip_range_end" || h == "end_ip")
            .ok_or_else(|| {
                TransformError::Enrichment(
                    "CSV missing required column 'ip_range_end' or 'end_ip'".to_string(),
                )
            })?;

        let country_code_idx = headers.iter().position(|h| h == "country_code");
        let country_name_idx = headers.iter().position(|h| h == "country_name");
        let city_idx = headers.iter().position(|h| h == "city");
        let region_idx = headers
            .iter()
            .position(|h| h == "stateprov" || h == "region");
        let lat_idx = headers.iter().position(|h| h == "latitude");
        let lon_idx = headers.iter().position(|h| h == "longitude");
        let asn_idx = headers.iter().position(|h| h == "asn");
        let org_idx = headers
            .iter()
            .position(|h| h == "org" || h == "organization");

        let mut ranges: Vec<RangeEntry> = Vec::new();

        for (row_num, record) in (1_u64..).zip(csv.records()) {
            let record = record.map_err(|e| {
                TransformError::Enrichment(format!("CSV parse error at row {row_num}: {e}"))
            })?;

            let get = |idx: usize| -> &str { record.get(idx).unwrap_or("").trim() };

            let start_str = get(start_idx);
            let end_str = get(end_idx);

            let start = parse_ip_to_u128(start_str).ok_or_else(|| {
                TransformError::Enrichment(format!(
                    "CSV row {row_num}: invalid start IP '{start_str}'"
                ))
            })?;
            let end = parse_ip_to_u128(end_str).ok_or_else(|| {
                TransformError::Enrichment(format!("CSV row {row_num}: invalid end IP '{end_str}'"))
            })?;

            if end < start {
                // Skip malformed rows silently — they are common in raw exports.
                continue;
            }

            let result = GeoResult {
                country_code: country_code_idx
                    .map(&get)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
                country_name: country_name_idx
                    .map(&get)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
                city: city_idx
                    .map(&get)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
                region: region_idx
                    .map(&get)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
                latitude: lat_idx.and_then(|i| get(i).parse::<f64>().ok()),
                longitude: lon_idx.and_then(|i| get(i).parse::<f64>().ok()),
                asn: asn_idx.and_then(|i| get(i).trim_start_matches("AS").parse::<i64>().ok()),
                org: org_idx
                    .map(get)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
            };

            ranges.push(RangeEntry { start, end, result });
        }

        // Sort by start address for binary search.
        ranges.sort_unstable_by_key(|e| e.start);

        // Reject overlapping ranges — they cause ambiguous lookups with binary search.
        for pair in ranges.windows(2) {
            if pair[0].end >= pair[1].start {
                return Err(TransformError::Enrichment(format!(
                    "CSV geo database contains overlapping IP ranges: range ending at {} overlaps range starting at {}",
                    u128_to_ip_string(pair[0].end),
                    u128_to_ip_string(pair[1].start),
                )));
            }
        }

        Ok(CsvRangeDatabase { ranges })
    }

    /// Load from a file path.
    pub fn open(path: &Path) -> Result<Self, TransformError> {
        let file = std::fs::File::open(path).map_err(|e| {
            TransformError::Enrichment(format!(
                "failed to open CSV geo database '{}': {e}",
                path.display()
            ))
        })?;
        Self::load_from_reader(io::BufReader::new(file))
    }

    /// Number of IP ranges loaded.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Returns `true` if no ranges were loaded.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

impl GeoDatabase for CsvRangeDatabase {
    fn lookup(&self, ip: &str) -> Option<GeoResult> {
        let addr = IpAddr::from_str(ip.trim()).ok()?;
        let key = ip_to_u128(addr);

        // Find the last entry whose start ≤ key.
        let idx = self
            .ranges
            .partition_point(|e| e.start <= key)
            .checked_sub(1)?;

        let entry = &self.ranges[idx];
        (entry.end >= key).then(|| entry.result.clone())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert an [`IpAddr`] to a comparable `u128`.
///
/// IPv4 addresses are mapped to `::ffff:0:0/96` so they sort before IPv6
/// addresses and are comparable with IPv4-mapped ranges in the CSV.
fn ip_to_u128(addr: IpAddr) -> u128 {
    match addr {
        IpAddr::V4(v4) => u128::from(v4.to_ipv6_mapped()),
        IpAddr::V6(v6) => u128::from(v6),
    }
}

/// Parse an IP string to `u128`, accepting both IPv4 and IPv6.
fn parse_ip_to_u128(s: &str) -> Option<u128> {
    IpAddr::from_str(s.trim()).ok().map(ip_to_u128)
}

/// Convert a `u128` back to an IP string for error messages.
///
/// Values in the IPv4-mapped range (`::ffff:0:0/96`) are rendered as IPv4.
fn u128_to_ip_string(val: u128) -> String {
    let addr = std::net::Ipv6Addr::from(val);
    match addr.to_ipv4_mapped() {
        Some(v4) => v4.to_string(),
        None => addr.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SIMPLE_CSV: &[u8] = b"\
ip_range_start,ip_range_end,country_code,country_name,city,stateprov,latitude,longitude,asn,org\n\
1.0.0.0,1.0.0.255,AU,Australia,,,,,7545,Optus\n\
8.8.8.0,8.8.8.255,US,United States,Mountain View,California,37.386,-122.084,15169,Google LLC\n\
2001:4860::,2001:4860:ffff:ffff:ffff:ffff:ffff:ffff,US,United States,,,,,,\n\
";

    #[test]
    fn load_from_reader_parses_rows() {
        let db = CsvRangeDatabase::load_from_reader(SIMPLE_CSV).unwrap();
        assert_eq!(db.len(), 3);
    }

    #[test]
    fn lookup_ipv4_hit() {
        let db = CsvRangeDatabase::load_from_reader(SIMPLE_CSV).unwrap();
        let result = db.lookup("8.8.8.8").unwrap();
        assert_eq!(result.country_code.as_deref(), Some("US"));
        assert_eq!(result.city.as_deref(), Some("Mountain View"));
        assert_eq!(result.asn, Some(15169));
        assert_eq!(result.org.as_deref(), Some("Google LLC"));
        assert!((result.latitude.unwrap() - 37.386).abs() < 0.001);
    }

    #[test]
    fn lookup_ipv4_first_range() {
        let db = CsvRangeDatabase::load_from_reader(SIMPLE_CSV).unwrap();
        let result = db.lookup("1.0.0.1").unwrap();
        assert_eq!(result.country_code.as_deref(), Some("AU"));
        assert_eq!(result.org.as_deref(), Some("Optus"));
    }

    #[test]
    fn lookup_ipv4_miss() {
        let db = CsvRangeDatabase::load_from_reader(SIMPLE_CSV).unwrap();
        assert!(db.lookup("5.5.5.5").is_none()); // not in any range
    }

    #[test]
    fn lookup_ipv6_hit() {
        let db = CsvRangeDatabase::load_from_reader(SIMPLE_CSV).unwrap();
        let result = db.lookup("2001:4860::1").unwrap();
        assert_eq!(result.country_code.as_deref(), Some("US"));
    }

    #[test]
    fn lookup_private_ip_miss() {
        let db = CsvRangeDatabase::load_from_reader(SIMPLE_CSV).unwrap();
        assert!(db.lookup("192.168.1.1").is_none());
        assert!(db.lookup("10.0.0.1").is_none());
    }

    #[test]
    fn lookup_malformed_ip_returns_none() {
        let db = CsvRangeDatabase::load_from_reader(SIMPLE_CSV).unwrap();
        assert!(db.lookup("not-an-ip").is_none());
        assert!(db.lookup("").is_none());
    }

    #[test]
    fn range_boundaries_inclusive() {
        let db = CsvRangeDatabase::load_from_reader(SIMPLE_CSV).unwrap();
        // Exact start and end of the AU range.
        assert!(db.lookup("1.0.0.0").is_some());
        assert!(db.lookup("1.0.0.255").is_some());
        assert!(db.lookup("1.0.1.0").is_none());
    }

    #[test]
    fn missing_start_column_returns_error() {
        let bad = b"ip_range_end,country_code\n1.0.0.255,AU\n";
        let result = CsvRangeDatabase::load_from_reader(&bad[..]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ip_range_start"));
    }

    #[test]
    fn asn_with_as_prefix_parsed() {
        let csv = b"ip_range_start,ip_range_end,country_code,asn\n1.2.3.0,1.2.3.255,US,AS12345\n";
        let db = CsvRangeDatabase::load_from_reader(&csv[..]).unwrap();
        let result = db.lookup("1.2.3.1").unwrap();
        assert_eq!(result.asn, Some(12345));
    }

    #[test]
    fn start_ip_end_ip_aliases() {
        let csv = b"start_ip,end_ip,country_code\n9.9.9.0,9.9.9.255,FR\n";
        let db = CsvRangeDatabase::load_from_reader(&csv[..]).unwrap();
        let result = db.lookup("9.9.9.9").unwrap();
        assert_eq!(result.country_code.as_deref(), Some("FR"));
    }

    #[test]
    fn is_empty_before_any_rows() {
        let csv = b"ip_range_start,ip_range_end,country_code\n";
        let db = CsvRangeDatabase::load_from_reader(&csv[..]).unwrap();
        assert!(db.is_empty());
    }

    #[test]
    fn overlapping_ranges_rejected() {
        let csv = b"ip_range_start,ip_range_end,country_code\n\
            1.0.0.0,1.0.0.255,AU\n\
            1.0.0.128,1.0.1.255,NZ\n";
        let result = CsvRangeDatabase::load_from_reader(&csv[..]);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("overlapping"),
            "expected overlap error, got: {msg}"
        );
    }

    #[test]
    fn adjacent_ranges_accepted() {
        let csv = b"ip_range_start,ip_range_end,country_code\n\
            1.0.0.0,1.0.0.127,AU\n\
            1.0.0.128,1.0.0.255,NZ\n";
        let db = CsvRangeDatabase::load_from_reader(&csv[..]).unwrap();
        assert_eq!(db.len(), 2);
        assert_eq!(
            db.lookup("1.0.0.1").unwrap().country_code.as_deref(),
            Some("AU")
        );
        assert_eq!(
            db.lookup("1.0.0.200").unwrap().country_code.as_deref(),
            Some("NZ")
        );
    }
}
