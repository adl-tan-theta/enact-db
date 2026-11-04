//! Arrow 58 (iceberg-rust) -> Arrow 59 (DataFusion 55) via the Arrow IPC
//! stream format. Both sides implement the same specification, so this is
//! lossless for every type, including field metadata (Iceberg field ids).

use anyhow::{anyhow, Result};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use std::io::Cursor;

pub fn schema(s: &arrow_schema58::Schema) -> Result<SchemaRef> {
    let mut buf = vec![];
    {
        let mut w = arrow_ipc58::writer::StreamWriter::try_new(&mut buf, s)?;
        w.finish()?;
    }
    let reader = StreamReader::try_new(Cursor::new(buf), None)?;
    Ok(reader.schema())
}

pub fn batch(b: &arrow_array58::RecordBatch) -> Result<RecordBatch> {
    let mut buf = Vec::with_capacity(b.get_array_memory_size() + 1024);
    {
        let mut w = arrow_ipc58::writer::StreamWriter::try_new(&mut buf, &b.schema())?;
        w.write(b)?;
        w.finish()?;
    }
    let mut reader = StreamReader::try_new(Cursor::new(buf), None)?;
    match reader.next() {
        Some(r) => Ok(r?),
        None => Err(anyhow!("arrow bridge: empty IPC stream")),
    }
}
