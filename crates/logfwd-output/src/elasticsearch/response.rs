//! Elasticsearch bulk response parsing and classification.

use std::io;

use super::types::{BulkItemResult, ElasticsearchSink};

impl ElasticsearchSink {
    /// Extract the `took` field (milliseconds) from an ES bulk response body.
    /// Returns `None` if the field is absent or not parseable.
    pub(super) fn extract_took(body: &[u8]) -> Option<u64> {
        const PREFIX: &[u8] = b"\"took\"";
        let pos = memchr::memmem::find(body, PREFIX)?;
        let rest = body[pos + PREFIX.len()..].trim_ascii_start();
        if !rest.starts_with(b":") {
            return None;
        }
        let rest = rest[1..].trim_ascii_start();
        let end = rest
            .iter()
            .position(|b| !b.is_ascii_digit())
            .unwrap_or(rest.len());
        if end == 0 {
            return None;
        }
        std::str::from_utf8(&rest[..end]).ok()?.parse().ok()
    }

    /// Parse the ES bulk API response body for per-document outcomes.
    ///
    /// The hot success path only deserializes the top-level `errors` flag.
    /// Full `items` parsing happens only for `errors:true`, where row-level
    /// retry/reject state is needed to avoid resending already accepted rows.
    pub(super) fn parse_bulk_response_detailed(body: &[u8]) -> io::Result<BulkItemResult> {
        #[derive(serde::Deserialize)]
        struct BulkHeader {
            errors: bool,
        }

        let header: BulkHeader = serde_json::from_slice(body).map_err(|e| {
            io::Error::other(format!("failed to parse ES bulk response header: {e}"))
        })?;

        if !header.errors {
            return Ok(BulkItemResult::default());
        }

        // Only do full parse on the error path to avoid hot-path allocations.
        let v: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| io::Error::other(format!("failed to parse ES bulk response: {e}")))?;

        let items = v
            .get("items")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| io::Error::other("ES bulk response missing 'items' array"))?;

        if items.is_empty() {
            return Err(io::Error::other(
                "ES bulk response indicated errors but 'items' array is empty",
            ));
        }

        let mut result = BulkItemResult {
            item_count: items.len(),
            ..BulkItemResult::default()
        };

        for (idx, item) in items.iter().enumerate() {
            let action = item
                .as_object()
                .and_then(|obj| obj.values().next())
                .and_then(serde_json::Value::as_object);
            if let Some(action_obj) = action {
                let status = action_obj
                    .get("status")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        io::Error::other("ES bulk response item missing numeric status field")
                    })?;

                if let Some(error) = action_obj.get("error") {
                    let error_type = error
                        .get("type")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown");
                    let reason = error
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("no reason provided");
                    if status == 429 || (500..600).contains(&status) {
                        result.retry_items.push(idx);
                        continue;
                    }
                    result.permanent_errors.push(format!(
                        "item {idx}: ES bulk error (status {status}): {error_type}: {reason}"
                    ));
                    continue;
                }

                // Some ES responses include only `status` for failed items. Preserve
                // retry semantics for retryable status-only failures.
                if status == 429 || (500..600).contains(&status) {
                    result.retry_items.push(idx);
                    continue;
                }
                if status >= 400 {
                    result.permanent_errors.push(format!(
                        "item {idx}: ES bulk error (status {status}): missing item error details"
                    ));
                } else if status >= 300 {
                    result.permanent_errors.push(format!(
                        "item {idx}: unexpected ES bulk item status {status}"
                    ));
                }
            }
        }

        if result.retry_items.is_empty() && result.permanent_errors.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ES bulk response indicated errors but no error details found in items",
            ));
        }

        Ok(result)
    }

    /// Compatibility wrapper used by tests that only need coarse error classification.
    #[cfg(test)]
    pub(super) fn parse_bulk_response(body: &[u8]) -> io::Result<()> {
        let result = Self::parse_bulk_response_detailed(body)?;
        let failed = result
            .retry_items
            .len()
            .saturating_add(result.permanent_errors.len());
        let succeeded = result.item_count.saturating_sub(failed);

        if succeeded > 0 && failed > 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "ES bulk partial failure: {succeeded} succeeded, \
                     {} retryable, {} rejected; \
                     not retrying to prevent duplication of {succeeded} delivered rows. \
                     First error: {}",
                    result.retry_items.len(),
                    result.permanent_errors.len(),
                    result
                        .permanent_errors
                        .first()
                        .map_or("retryable item failure", String::as_str)
                ),
            ));
        }

        if !result.retry_items.is_empty() && result.permanent_errors.is_empty() {
            return Err(io::Error::other(format!(
                "ES bulk transient error: all {} items failed",
                result.retry_items.len()
            )));
        }

        if !result.permanent_errors.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "ES bulk error: all {} items rejected. Errors: {}",
                    result.permanent_errors.len(),
                    result.permanent_errors.join("; ")
                ),
            ));
        }

        Ok(())
    }

    pub(super) fn parse_success_item_count(body: &[u8]) -> io::Result<Option<usize>> {
        #[derive(serde::Deserialize)]
        struct BulkResponseItems {
            errors: bool,
            items: Option<Vec<serde_json::Value>>,
        }

        let response: BulkResponseItems = serde_json::from_slice(body).map_err(|e| {
            io::Error::other(format!("failed to parse ES bulk response items: {e}"))
        })?;
        if response.errors {
            return Ok(None);
        }
        Ok(response.items.map(|items| items.len()))
    }
}
