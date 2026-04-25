pub(super) fn classify_projection_support(input: &[u8]) -> Result<(), ProjectionError> {
    scan_message(input, MessageKind::ExportLogsServiceRequest)
}

pub(super) struct OtlpFieldHandles {
    pub(super) timestamp: FieldHandle,
    pub(super) observed_timestamp: FieldHandle,
    pub(super) severity: FieldHandle,
    pub(super) severity_number: FieldHandle,
    pub(super) body: FieldHandle,
    pub(super) trace_id: FieldHandle,
    pub(super) span_id: FieldHandle,
    pub(super) flags: FieldHandle,
    pub(super) scope_name: FieldHandle,
    pub(super) scope_version: FieldHandle,
}

impl OtlpFieldHandles {
    pub(super) fn write_timestamp(&self, builder: &mut ColumnarBatchBuilder, value: i64) {
        builder.write_i64(self.timestamp, value);
    }

    pub(super) fn write_observed_timestamp(&self, builder: &mut ColumnarBatchBuilder, value: i64) {
        builder.write_i64(self.observed_timestamp, value);
    }

    pub(super) fn write_severity(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: &[u8],
        string_storage: StringStorage,
    ) -> Result<(), ProjectionError> {
        super::write_wire_str(builder, self.severity, value, string_storage)
    }

    pub(super) fn write_severity_number(&self, builder: &mut ColumnarBatchBuilder, value: i64) {
        builder.write_i64(self.severity_number, value);
    }

    pub(super) fn write_body(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: WireAny<'_>,
        scratch: &mut WireScratch,
        string_storage: StringStorage,
    ) -> Result<(), ProjectionError> {
        write_wire_any_as_string(builder, self.body, value, scratch, string_storage)
    }

    pub(super) fn write_trace_id(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: &[u8],
    ) -> Result<(), ProjectionError> {
        super::write_hex_field(builder, self.trace_id, value)
    }

    pub(super) fn write_span_id(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: &[u8],
    ) -> Result<(), ProjectionError> {
        super::write_hex_field(builder, self.span_id, value)
    }

    pub(super) fn write_flags(&self, builder: &mut ColumnarBatchBuilder, value: i64) {
        builder.write_i64(self.flags, value);
    }

    pub(super) fn write_scope_name(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: &[u8],
        string_storage: StringStorage,
    ) -> Result<(), ProjectionError> {
        super::write_wire_str(builder, self.scope_name, value, string_storage)
    }

    pub(super) fn write_scope_version(
        &self,
        builder: &mut ColumnarBatchBuilder,
        value: &[u8],
        string_storage: StringStorage,
    ) -> Result<(), ProjectionError> {
        super::write_wire_str(builder, self.scope_version, value, string_storage)
    }
}

pub(super) fn build_otlp_plan() -> (BatchPlan, OtlpFieldHandles) {
    let mut plan = BatchPlan::new();
    let handles = OtlpFieldHandles {
        timestamp: plan
            .declare_planned(field_names::TIMESTAMP, FieldKind::Int64)
            .expect("duplicate planned field"),
        observed_timestamp: plan
            .declare_planned(field_names::OBSERVED_TIMESTAMP, FieldKind::Int64)
            .expect("duplicate planned field"),
        severity: plan
            .declare_planned(field_names::SEVERITY, FieldKind::Utf8View)
            .expect("duplicate planned field"),
        severity_number: plan
            .declare_planned(field_names::SEVERITY_NUMBER, FieldKind::Int64)
            .expect("duplicate planned field"),
        body: plan
            .declare_planned(field_names::BODY, FieldKind::Utf8View)
            .expect("duplicate planned field"),
        trace_id: plan
            .declare_planned(field_names::TRACE_ID, FieldKind::Utf8View)
            .expect("duplicate planned field"),
        span_id: plan
            .declare_planned(field_names::SPAN_ID, FieldKind::Utf8View)
            .expect("duplicate planned field"),
        flags: plan
            .declare_planned(field_names::FLAGS, FieldKind::Int64)
            .expect("duplicate planned field"),
        scope_name: plan
            .declare_planned(field_names::SCOPE_NAME, FieldKind::Utf8View)
            .expect("duplicate planned field"),
        scope_version: plan
            .declare_planned(field_names::SCOPE_VERSION, FieldKind::Utf8View)
            .expect("duplicate planned field"),
    };
    (plan, handles)
}

#[derive(Clone, Copy, Default)]
pub(super) struct ScopeFields<'a> {
    pub(super) name: Option<&'a [u8]>,
    pub(super) version: Option<&'a [u8]>,
}

pub(super) fn merge_scope_wire<'a>(
    scope: &'a [u8],
    scope_fields: &mut ScopeFields<'a>,
) -> Result<(), ProjectionError> {
    super::for_each_field(scope, |field, value| {
        match (field, value) {
            (1, WireField::Len(value)) => {
                scope_fields.name = Some(super::require_utf8(value, "invalid UTF-8 scope name")?);
            }
            (1, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for InstrumentationScope.name",
                ));
            }
            (2, WireField::Len(value)) => {
                scope_fields.version =
                    Some(super::require_utf8(value, "invalid UTF-8 scope version")?);
            }
            (2, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for InstrumentationScope.version",
                ));
            }
            _ => {}
        }
        Ok(())
    })
}

#[derive(Clone, Copy, Default)]
pub(super) struct LogRecordFields<'a> {
    pub(super) time_unix_nano: u64,
    pub(super) observed_time_unix_nano: u64,
    pub(super) severity_number: i64,
    pub(super) severity_text: Option<&'a [u8]>,
    pub(super) body: Option<WireAny<'a>>,
    pub(super) trace_id: Option<&'a [u8]>,
    pub(super) span_id: Option<&'a [u8]>,
    pub(super) flags: i64,
}

pub(super) fn decode_log_record_fields<'a>(
    log_record: &'a [u8],
    attr_ranges: &mut Vec<(usize, usize)>,
) -> Result<LogRecordFields<'a>, ProjectionError> {
    let mut out = LogRecordFields::default();
    super::for_each_field(log_record, |field, value| {
        match (field, value) {
            (1, WireField::Fixed64(value)) => {
                out.time_unix_nano = value;
            }
            (1, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.time_unix_nano",
                ));
            }
            (11, WireField::Fixed64(value)) => {
                out.observed_time_unix_nano = value;
            }
            (11, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.observed_time_unix_nano",
                ));
            }
            (2, WireField::Varint(value)) => {
                out.severity_number = i64::from(value as i32);
            }
            (2, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.severity_number",
                ));
            }
            (3, WireField::Len(value)) => {
                out.severity_text = if value.is_empty() {
                    None
                } else {
                    Some(super::require_utf8(value, "invalid UTF-8 severity text")?)
                };
            }
            (3, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.severity_text",
                ));
            }
            (5, WireField::Len(value)) => {
                if let Some(value) = decode_any_value_wire(value)? {
                    out.body = Some(value);
                }
            }
            (5, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.body",
                ));
            }
            (7, WireField::Varint(_)) => {}
            (7, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.dropped_attributes_count",
                ));
            }
            (9, WireField::Len(value)) => {
                out.trace_id = (!value.is_empty()).then_some(value);
            }
            (9, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.trace_id",
                ));
            }
            (10, WireField::Len(value)) => {
                out.span_id = (!value.is_empty()).then_some(value);
            }
            (10, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.span_id",
                ));
            }
            (8, WireField::Fixed32(value)) => {
                out.flags = i64::from(value);
            }
            (8, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.flags",
                ));
            }
            (6, WireField::Len(value)) => {
                attr_ranges.push(super::subslice_range(log_record, value)?);
            }
            (6, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.attributes",
                ));
            }
            (12, WireField::Len(value)) if !value.is_empty() => {
                super::require_utf8(value, "invalid UTF-8 LogRecord.event_name")?;
            }
            (12, WireField::Len(_)) => {}
            (12, _) => {
                return Err(ProjectionError::Invalid(
                    "invalid wire type for LogRecord.event_name",
                ));
            }
            _ => {}
        }
        Ok(())
    })?;
    Ok(out)
}
