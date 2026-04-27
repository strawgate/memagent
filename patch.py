import os
import re

base = "/Users/billeaston/Documents/repos/fastforward-simd-experiment/"

def patch(path, old, new):
    with open(base + path, 'r') as f:
        content = f.read()
    if old not in content:
        raise Exception(f"Failed to find '{old}' in {path}")
    with open(base + path, 'w') as f:
        f.write(content.replace(old, new))

# 1. Cargo.toml features
patch("crates/ffwd-io/Cargo.toml", "otlp-research = []\n", "otlp-research = []\notlp-warp = []\n")
patch("crates/ffwd-io/Cargo.toml", "xxhash-rust = { workspace = true }\n", "xxhash-rust = { workspace = true }\nwide = \"1\"\n")
patch("crates/ffwd-bench/Cargo.toml", "otlp-profile-alloc = []\n", "otlp-profile-alloc = []\notlp-warp = [\"ffwd-io/otlp-warp\"]\n")

# 2. accumulator.rs
reserve_impl = """
    pub fn reserve(&mut self, additional: usize) {
        match self {
            ColumnAccumulator::Int64 { facts, .. } => facts.reserve(additional),
            ColumnAccumulator::Float64 { facts, .. } => facts.reserve(additional),
            ColumnAccumulator::Bool { facts, .. } => facts.reserve(additional),
            ColumnAccumulator::String { facts, .. } => facts.reserve(additional),
            ColumnAccumulator::Dynamic { int_facts, float_facts, bool_facts, str_facts, .. } => {
                int_facts.reserve(additional);
                float_facts.reserve(additional);
                bool_facts.reserve(additional);
                str_facts.reserve(additional);
            }
        }
    }
"""
patch("crates/ffwd-arrow/src/columnar/accumulator.rs", "    pub fn clear(&mut self) {", reserve_impl + "    pub fn clear(&mut self) {")

# 3. builder.rs
reserve_mandatory = """
    pub fn reserve_mandatory(&mut self, additional: usize) {
        for i in [0, 4] { if let Some(col) = self.columns.get_mut(i) { col.reserve(additional); } }
    }
    pub fn reserve_string_buf(&mut self, additional: usize) {
        self.string_buf.reserve(additional);
    }
"""
patch("crates/ffwd-arrow/src/columnar/builder.rs", "    pub fn begin_batch(&mut self) {", reserve_mandatory + "    pub fn begin_batch(&mut self) {")

# 4. wire.rs
with open(base + "crates/ffwd-io/src/otlp_receiver/projection/wire.rs", 'r') as f:
    wire_content = f.read()
wire_content = "use wide::u8x16;\n" + wire_content
wire_simd = """
pub(super) fn count_structural_density_simd(input: &[u8]) -> (usize, usize) {
    let logs = memchr::memchr_iter(0x12, input).count();
    let attrs = memchr::memchr_iter(0x32, input).count();
    (logs, attrs)
}

pub(super) fn try_decode_log_record_warp(
    input: &[u8],
    out_ts: &mut Option<u64>,
    out_obs_ts: &mut Option<u64>,
    out_sev: &mut Option<u64>,
) -> Option<usize> {
    if input.len() < 21 { return None; }
    if input[0] == 0x09 && input[9] == 0x59 && input[18] == 0x10 {
        if input[19] < 128 {
            *out_ts = Some(u64::from_le_bytes(input[1..9].try_into().unwrap()));
            *out_obs_ts = Some(u64::from_le_bytes(input[10..18].try_into().unwrap()));
            *out_sev = Some(input[19] as u64);
            return Some(20);
        }
    }
    None
}
"""
with open(base + "crates/ffwd-io/src/otlp_receiver/projection/wire.rs", 'w') as f:
    f.write(wire_content + wire_simd)

# 5. projection.rs
patch("crates/ffwd-io/src/otlp_receiver/projection.rs", 
      "pub fn decode_view_bytes(&mut self, body: Bytes) -> Result<RecordBatch, InputError> {", 
      "pub fn decode_view_bytes(&mut self, body: Bytes, warp_enabled: bool) -> Result<RecordBatch, InputError> {\n        self.try_decode_view_bytes(body, warp_enabled)\n            .map_err(ProjectionError::into_input_error)\n    }\n\n    pub(super) fn decode_view_bytes_orig(&mut self, body: Bytes) -> Result<RecordBatch, InputError> {")
patch("crates/ffwd-io/src/otlp_receiver/projection.rs",
      "pub(super) fn try_decode_view_bytes(\n        &mut self,\n        body: Bytes,\n    ) -> Result<RecordBatch, ProjectionError> {",
      "pub(super) fn try_decode_view_bytes(\n        &mut self,\n        body: Bytes,\n        warp_enabled: bool,\n    ) -> Result<RecordBatch, ProjectionError> {")
patch("crates/ffwd-io/src/otlp_receiver/projection.rs",
      "self.builder.begin_batch();\n        if !backing.is_empty() {\n            self.builder\n                .set_original_buffer(arrow::buffer::Buffer::from(backing));\n        }",
      "self.builder.begin_batch();\n        if !backing.is_empty() {\n            self.builder\n                .set_original_buffer(arrow::buffer::Buffer::from(backing));\n            if warp_enabled {\n                let (log_count, _) = wire::count_structural_density_simd(&body);\n                if log_count > 256 {\n                    self.builder.reserve_mandatory(log_count);\n                    self.builder.reserve_string_buf(body.len() / 2);\n                }\n            }\n        }")
patch("crates/ffwd-io/src/otlp_receiver/projection.rs",
      "decode::decode_resource_logs_wire(\n                    &mut self.builder,\n                    &self.handles,\n                    &mut self.scratch,\n                    &self.resource_prefix,\n                    resource_logs,\n                    string_storage,\n                )",
      "decode::decode_resource_logs_wire(\n                    &mut self.builder,\n                    &self.handles,\n                    &mut self.scratch,\n                    &self.resource_prefix,\n                    resource_logs,\n                    string_storage,\n                    warp_enabled,\n                )")

# 6. decode.rs
patch("crates/ffwd-io/src/otlp_receiver/projection/decode.rs",
      "pub(super) fn decode_resource_logs_wire(\n    builder: &mut ColumnarBatchBuilder,\n    fields: &generated::OtlpFieldHandles,\n    scratch: &mut WireScratch,\n    resource_prefix: &str,\n    resource_logs: &[u8],\n    string_storage: StringStorage,\n) -> Result<(), ProjectionError> {",
      "pub(super) fn decode_resource_logs_wire(\n    builder: &mut ColumnarBatchBuilder,\n    fields: &generated::OtlpFieldHandles,\n    scratch: &mut WireScratch,\n    resource_prefix: &str,\n    resource_logs: &[u8],\n    string_storage: StringStorage,\n    warp_enabled: bool,\n) -> Result<(), ProjectionError> {")
patch("crates/ffwd-io/src/otlp_receiver/projection/decode.rs",
      "decode_scope_logs_wire(\n            builder,\n            fields,\n            scratch,\n            &resource_attrs,\n            scope_logs,\n            string_storage,\n        )",
      "decode_scope_logs_wire(\n            builder,\n            fields,\n            scratch,\n            &resource_attrs,\n            scope_logs,\n            string_storage,\n            warp_enabled,\n        )")
patch("crates/ffwd-io/src/otlp_receiver/projection/decode.rs",
      "fn decode_scope_logs_wire(\n    builder: &mut ColumnarBatchBuilder,\n    fields: &generated::OtlpFieldHandles,\n    scratch: &mut WireScratch,\n    resource_attrs: &[(FieldHandle, WireAny<'_>)],\n    scope_logs: &[u8],\n    string_storage: StringStorage,\n) -> Result<(), ProjectionError> {",
      "fn decode_scope_logs_wire(\n    builder: &mut ColumnarBatchBuilder,\n    fields: &generated::OtlpFieldHandles,\n    scratch: &mut WireScratch,\n    resource_attrs: &[(FieldHandle, WireAny<'_>)],\n    scope_logs: &[u8],\n    string_storage: StringStorage,\n    warp_enabled: bool,\n) -> Result<(), ProjectionError> {")
patch("crates/ffwd-io/src/otlp_receiver/projection/decode.rs",
      "decode_log_record_wire(\n            builder,\n            fields,\n            scratch,\n            resource_attrs,\n            scope_fields,\n            log_record,\n            string_storage,\n        )",
      "decode_log_record_wire(\n            builder,\n            fields,\n            scratch,\n            resource_attrs,\n            scope_fields,\n            log_record,\n            string_storage,\n            warp_enabled,\n        )")
patch("crates/ffwd-io/src/otlp_receiver/projection/decode.rs",
      "fn decode_log_record_wire(\n    builder: &mut ColumnarBatchBuilder,\n    fields: &generated::OtlpFieldHandles,\n    scratch: &mut WireScratch,\n    resource_attrs: &[(FieldHandle, WireAny<'_>)],\n    scope_fields: generated::ScopeFields<'_>,\n    log_record: &[u8],\n    string_storage: StringStorage,\n) -> Result<(), ProjectionError> {",
      "fn decode_log_record_wire(\n    builder: &mut ColumnarBatchBuilder,\n    fields: &generated::OtlpFieldHandles,\n    scratch: &mut WireScratch,\n    resource_attrs: &[(FieldHandle, WireAny<'_>)],\n    scope_fields: generated::ScopeFields<'_>,\n    log_record: &[u8],\n    string_storage: StringStorage,\n    warp_enabled: bool,\n) -> Result<(), ProjectionError> {")

warp_logic = """
    let mut warp_ts = None;
    let mut warp_obs_ts = None;
    let mut warp_sev = None;
    let (mut record, _) = if warp_enabled 
        && let Some(consumed) = super::wire::try_decode_log_record_warp(
            log_record,
            &mut warp_ts,
            &mut warp_obs_ts,
            &mut warp_sev,
        ) 
    {
        let rec = generated::decode_log_record_fields(&log_record[consumed..], &mut scratch.attr_ranges)?;
        (rec, &log_record[consumed..])
    } else {
        let rec = generated::decode_log_record_fields(log_record, &mut scratch.attr_ranges)?;
        (rec, log_record)
    };

    if let Some(v) = warp_ts { record.time_unix_nano = v; }
    if let Some(v) = warp_obs_ts { record.observed_time_unix_nano = v; }
    if let Some(v) = warp_sev { record.severity_number = v as i64; }
"""
patch("crates/ffwd-io/src/otlp_receiver/projection/decode.rs",
      "let record = generated::decode_log_record_fields(log_record, &mut scratch.attr_ranges)?;\n",
      warp_logic)

# 7. otlp_io.rs (benchmark)
bench_patch = """
        group.bench_with_input(
            BenchmarkId::new("projected_warp_to_batch", profile.name),
            &fixture.payload_bytes,
            |b, payload| {
                let mut decoder = ProjectedOtlpDecoder::new("resource.");
                let _ = decoder.decode_view_bytes(payload.clone(), true).unwrap();
                b.iter(|| {
                    for _ in 0..batches_per_iter {
                        let batch = decoder
                            .decode_view_bytes(payload.clone(), true)
                            .expect("repeated warp decode should succeed");
                        std::hint::black_box(batch.num_rows());
                    }
                });
            },
        );
"""
patch("crates/ffwd-bench/benches/otlp_io.rs",
      "                        std::hint::black_box(batch.num_rows());\n                    }\n                });\n            },\n        );",
      "                        std::hint::black_box(batch.num_rows());\n                    }\n                });\n            },\n        );\n" + bench_patch)

print("Patching successful.")
