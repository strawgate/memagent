//! Arrow IPC serialization/deserialization for RecordBatch persistence.
//!
//! Uses the Arrow IPC stream format: self-describing (schema is embedded),
//! supports schema evolution, and is the standard way to shuttle Arrow
//! RecordBatches across process or storage boundaries.
//!
//! This is used by the disk-queue and memory-multi pipeline modes to pass
//! batches between the collector thread and the processor thread.

use std::io;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Serialize a [`RecordBatch`] to Arrow IPC stream-format bytes.
///
/// The returned bytes are self-describing: they include the schema and a
/// single record-batch message.  Use [`ipc_to_batch`] to round-trip.
pub fn batch_to_ipc(batch: &RecordBatch) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buf, batch.schema_ref()).map_err(io::Error::other)?;
    writer.write(batch).map_err(io::Error::other)?;
    writer.finish().map_err(io::Error::other)?;
    Ok(buf)
}

/// Deserialize Arrow IPC stream-format bytes back to a [`RecordBatch`].
///
/// Returns the first (and typically only) batch in the stream.
/// The schema is recovered automatically from the embedded header.
pub fn ipc_to_batch(data: &[u8]) -> io::Result<RecordBatch> {
    let cursor = io::Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None).map_err(io::Error::other)?;
    reader
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty IPC stream"))?
        .map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level_str", DataType::Utf8, true),
            Field::new("status_int", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["INFO", "WARN", "ERROR"])),
                Arc::new(Int64Array::from(vec![200i64, 429, 500])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn roundtrip() {
        let batch = make_batch();
        let ipc = batch_to_ipc(&batch).unwrap();
        assert!(!ipc.is_empty());
        let recovered = ipc_to_batch(&ipc).unwrap();
        assert_eq!(recovered.num_rows(), 3);
        assert_eq!(recovered.num_columns(), 2);
        assert_eq!(recovered.schema(), batch.schema());
    }

    #[test]
    fn schema_preserved() {
        let batch = make_batch();
        let recovered = ipc_to_batch(&batch_to_ipc(&batch).unwrap()).unwrap();
        let schema = recovered.schema();
        assert_eq!(schema.field(0).name(), "level_str");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).name(), "status_int");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn empty_data_rejected() {
        // Arrow IPC reader wraps its errors via io::Error::other, so we just
        // check that empty input is rejected — not the exact ErrorKind.
        assert!(ipc_to_batch(&[]).is_err());
    }

    #[test]
    fn truncated_ipc_rejected() {
        // A valid IPC stream cut short (just the magic bytes, no schema).
        let partial = b"ARROW1\0\0"; // Arrow IPC magic + padding, no schema
        assert!(ipc_to_batch(partial).is_err());
    }
}
