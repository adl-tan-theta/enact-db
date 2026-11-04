//! Arrow → DuckDB `LogicalType` / `DataChunk` / `Vector` encoding.
//!
//! Mirrors duckdb/src/common/types/vector.cpp (`Vector::Serialize`) and
//! data_chunk.cpp with Quack's `StorageVersion::V2_0_0` serialization options:
//! flat vectors, validity as u64 words (bit set = valid), constant-size types
//! as raw little-endian buffers, VARCHAR/BLOB as (107 byte length, 108 u32
//! lengths, 109 bytes), STRUCT as child vectors, LIST as entries + child.

use super::wire::Writer;
use anyhow::{bail, Result};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, GenericListArray, OffsetSizeTrait, StructArray,
};
use datafusion::arrow::compute::{can_cast_types, cast};
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// DuckDB's STANDARD_VECTOR_SIZE: a DataChunk never holds more rows.
pub const VECTOR_SIZE: usize = 2048;

#[derive(Debug, Clone, PartialEq)]
pub enum DuckType {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    UTinyInt,
    USmallInt,
    UInteger,
    UBigInt,
    Float,
    Double,
    Varchar,
    Blob,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Decimal(u8, u8),
    Interval,
    List(Box<DuckType>),
    Struct(Vec<(String, DuckType)>),
}

impl DuckType {
    /// LogicalTypeId values from duckdb/common/types.hpp.
    fn id(&self) -> u8 {
        match self {
            DuckType::Boolean => 10,
            DuckType::TinyInt => 11,
            DuckType::SmallInt => 12,
            DuckType::Integer => 13,
            DuckType::BigInt => 14,
            DuckType::Date => 15,
            DuckType::Time => 16,
            DuckType::Timestamp => 19,
            DuckType::Decimal(..) => 21,
            DuckType::Float => 22,
            DuckType::Double => 23,
            DuckType::Varchar => 25,
            DuckType::Blob => 26,
            DuckType::Interval => 27,
            DuckType::UTinyInt => 28,
            DuckType::USmallInt => 29,
            DuckType::UInteger => 30,
            DuckType::UBigInt => 31,
            DuckType::TimestampTz => 32,
            DuckType::Struct(_) => 100,
            DuckType::List(_) => 101,
        }
    }

    /// The type as DuckDB DDL (used in the CREATE TABLE statements the client binds).
    pub fn ddl(&self) -> String {
        match self {
            DuckType::Boolean => "BOOLEAN".into(),
            DuckType::TinyInt => "TINYINT".into(),
            DuckType::SmallInt => "SMALLINT".into(),
            DuckType::Integer => "INTEGER".into(),
            DuckType::BigInt => "BIGINT".into(),
            DuckType::UTinyInt => "UTINYINT".into(),
            DuckType::USmallInt => "USMALLINT".into(),
            DuckType::UInteger => "UINTEGER".into(),
            DuckType::UBigInt => "UBIGINT".into(),
            DuckType::Float => "FLOAT".into(),
            DuckType::Double => "DOUBLE".into(),
            DuckType::Varchar => "VARCHAR".into(),
            DuckType::Blob => "BLOB".into(),
            DuckType::Date => "DATE".into(),
            DuckType::Time => "TIME".into(),
            DuckType::Timestamp => "TIMESTAMP".into(),
            DuckType::TimestampTz => "TIMESTAMPTZ".into(),
            DuckType::Decimal(p, s) => format!("DECIMAL({p},{s})"),
            DuckType::Interval => "INTERVAL".into(),
            DuckType::List(c) => format!("{}[]", c.ddl()),
            DuckType::Struct(fs) => format!(
                "STRUCT({})",
                fs.iter().map(|(n, t)| format!("{} {}", quote_ident(n), t.ddl())).collect::<Vec<_>>().join(", ")
            ),
        }
    }

    /// The Arrow type a column is cast to before encoding.
    pub fn arrow(&self) -> DataType {
        match self {
            DuckType::Boolean => DataType::Boolean,
            DuckType::TinyInt => DataType::Int8,
            DuckType::SmallInt => DataType::Int16,
            DuckType::Integer => DataType::Int32,
            DuckType::BigInt => DataType::Int64,
            DuckType::UTinyInt => DataType::UInt8,
            DuckType::USmallInt => DataType::UInt16,
            DuckType::UInteger => DataType::UInt32,
            DuckType::UBigInt => DataType::UInt64,
            DuckType::Float => DataType::Float32,
            DuckType::Double => DataType::Float64,
            DuckType::Varchar => DataType::Utf8,
            DuckType::Blob => DataType::Binary,
            DuckType::Date => DataType::Date32,
            DuckType::Time => DataType::Time64(TimeUnit::Microsecond),
            DuckType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            DuckType::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DuckType::Decimal(p, s) => DataType::Decimal128(*p, *s as i8),
            DuckType::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
            DuckType::List(c) => DataType::List(Arc::new(Field::new("item", c.arrow(), true))),
            DuckType::Struct(fs) => DataType::Struct(
                fs.iter().map(|(n, t)| Field::new(n, t.arrow(), true)).collect::<Vec<_>>().into(),
            ),
        }
    }

    /// `LogicalType::Serialize` (id + optional type info) as one object.
    pub fn write(&self, w: &mut Writer) {
        w.field(100).uvar(self.id() as u64);
        match self {
            DuckType::Decimal(p, s) => {
                w.field(101).boolean(true); // nullable present
                w.field(100).uvar(2); // DECIMAL_TYPE_INFO
                if *p != 0 {
                    w.field(200).uvar(*p as u64);
                }
                if *s != 0 {
                    w.field(201).uvar(*s as u64);
                }
                w.end();
            }
            DuckType::List(c) => {
                w.field(101).boolean(true);
                w.field(100).uvar(4); // LIST_TYPE_INFO
                w.field(200);
                c.write(w);
                w.end();
            }
            DuckType::Struct(fs) => {
                w.field(101).boolean(true);
                w.field(100).uvar(5); // STRUCT_TYPE_INFO
                if !fs.is_empty() {
                    w.field(200).uvar(fs.len() as u64);
                    for (name, t) in fs {
                        // std::pair: {0: first, 1: second}
                        w.field(0).string(name);
                        w.field(1);
                        t.write(w);
                        w.end();
                    }
                }
                w.end();
            }
            _ => {}
        }
        w.end();
    }

    /// Physical width of constant-size types.
    fn width(&self) -> Option<usize> {
        Some(match self {
            DuckType::Boolean | DuckType::TinyInt | DuckType::UTinyInt => 1,
            DuckType::SmallInt | DuckType::USmallInt => 2,
            DuckType::Integer | DuckType::UInteger | DuckType::Float | DuckType::Date => 4,
            DuckType::BigInt
            | DuckType::UBigInt
            | DuckType::Double
            | DuckType::Time
            | DuckType::Timestamp
            | DuckType::TimestampTz => 8,
            DuckType::Decimal(p, _) => decimal_width(*p),
            DuckType::Interval => 16,
            _ => return None,
        })
    }
}

fn decimal_width(p: u8) -> usize {
    match p {
        0..=4 => 2,
        5..=9 => 4,
        10..=18 => 8,
        _ => 16,
    }
}

pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Pick the DuckDB type for an Arrow type (falling back to VARCHAR for anything
/// DuckDB can't receive natively or Arrow can't cast).
pub fn duck_type(dt: &DataType) -> DuckType {
    let t = match dt {
        DataType::Boolean => DuckType::Boolean,
        DataType::Int8 => DuckType::TinyInt,
        DataType::Int16 => DuckType::SmallInt,
        DataType::Int32 => DuckType::Integer,
        DataType::Int64 => DuckType::BigInt,
        DataType::UInt8 => DuckType::UTinyInt,
        DataType::UInt16 => DuckType::USmallInt,
        DataType::UInt32 => DuckType::UInteger,
        DataType::UInt64 => DuckType::UBigInt,
        DataType::Float16 | DataType::Float32 => DuckType::Float,
        DataType::Float64 => DuckType::Double,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DuckType::Varchar,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            DuckType::Blob
        }
        DataType::Date32 | DataType::Date64 => DuckType::Date,
        DataType::Time32(_) | DataType::Time64(_) => DuckType::Time,
        DataType::Timestamp(_, None) => DuckType::Timestamp,
        DataType::Timestamp(_, Some(_)) => DuckType::TimestampTz,
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s)
            if *s >= 0 && *p <= 38 && (*s as u8) <= *p =>
        {
            DuckType::Decimal((*p).max(1), *s as u8)
        }
        DataType::Decimal256(p, s) if *s >= 0 && *p <= 38 && (*s as u8) <= *p => DuckType::Decimal((*p).max(1), *s as u8),
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..) => {
            DuckType::Double
        }
        DataType::Interval(_) => DuckType::Interval,
        DataType::List(f) | DataType::LargeList(f) | DataType::ListView(f) | DataType::LargeListView(f) => {
            DuckType::List(Box::new(duck_type(f.data_type())))
        }
        DataType::FixedSizeList(f, _) => DuckType::List(Box::new(duck_type(f.data_type()))),
        DataType::Struct(fs) if !fs.is_empty() => {
            DuckType::Struct(fs.iter().map(|f| (f.name().clone(), duck_type(f.data_type()))).collect())
        }
        DataType::Dictionary(_, v) => duck_type(v),
        _ => DuckType::Varchar,
    };
    if t != DuckType::Varchar && !can_cast_types(dt, &t.arrow()) {
        return DuckType::Varchar;
    }
    t
}

pub fn duck_types(schema: &Schema) -> Vec<DuckType> {
    schema.fields().iter().map(|f| duck_type(f.data_type())).collect()
}

/// `CREATE TABLE "name"("col" TYPE, ...)` for the client's catalog.
pub fn create_table_sql(name: &str, schema: &Schema) -> String {
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("{} {}", quote_ident(f.name()), duck_type(f.data_type()).ddl()))
        .collect();
    format!("CREATE TABLE {}({});", quote_ident(name), cols.join(", "))
}

/// Cast a batch's columns to the arrays the encoder expects.
pub fn canonicalize(batch: &RecordBatch, types: &[DuckType]) -> Result<Vec<ArrayRef>> {
    batch
        .columns()
        .iter()
        .zip(types)
        .map(|(c, t)| {
            let target = t.arrow();
            if c.data_type() == &target {
                Ok(c.clone())
            } else {
                Ok(cast(c, &target)?)
            }
        })
        .collect()
}

/// Serialize columns (already canonical, ≤ VECTOR_SIZE rows) as one DataChunk:
/// fields only, the caller wraps it in an object / adds the terminator.
pub fn write_chunk(w: &mut Writer, cols: &[ArrayRef], types: &[DuckType]) -> Result<()> {
    let rows = cols.first().map(|c| c.len()).unwrap_or(0);
    if rows == 0 || rows > VECTOR_SIZE {
        bail!("quack: data chunk must hold 1..={VECTOR_SIZE} rows, got {rows}");
    }
    w.field(100).uvar(rows as u64); // sel_t rows
    w.field(101).uvar(types.len() as u64);
    for t in types {
        t.write(w);
    }
    w.field(102).uvar(cols.len() as u64);
    for (c, t) in cols.iter().zip(types) {
        write_vector(w, c.as_ref(), t)?;
        w.end();
    }
    Ok(())
}

fn validity(arr: &dyn Array) -> Option<Vec<u8>> {
    if arr.null_count() == 0 {
        return None;
    }
    let n = arr.len();
    let mut words = vec![0u64; n.div_ceil(64)];
    for i in 0..n {
        if arr.is_valid(i) {
            words[i / 64] |= 1u64 << (i % 64);
        }
    }
    Some(words.iter().flat_map(|w| w.to_le_bytes()).collect())
}

/// `Vector::Serialize` for a flat vector (fields only).
fn write_vector(w: &mut Writer, arr: &dyn Array, t: &DuckType) -> Result<()> {
    let n = arr.len();
    let mask = validity(arr);
    w.field(100).boolean(mask.is_some());
    if let Some(m) = &mask {
        w.field(101).blob(m);
    }
    if let Some(width) = t.width() {
        let mut data = Vec::with_capacity(width * n);
        fixed_bytes(arr, t, &mut data)?;
        debug_assert_eq!(data.len(), width * n);
        w.field(102).blob(&data);
        return Ok(());
    }
    match t {
        DuckType::Varchar | DuckType::Blob => {
            let mut lengths = Vec::with_capacity(4 * n);
            let mut bytes = Vec::new();
            for i in 0..n {
                let v: &[u8] = if arr.is_null(i) {
                    &[]
                } else if let Some(s) = arr.as_string_opt::<i32>() {
                    s.value(i).as_bytes()
                } else {
                    arr.as_binary::<i32>().value(i)
                };
                lengths.extend_from_slice(&(v.len() as u32).to_le_bytes());
                bytes.extend_from_slice(v);
            }
            w.field(107).uvar(bytes.len() as u64); // optional_idx byte_data_length
            w.field(108).blob(&lengths);
            w.field(109).blob(&bytes);
        }
        DuckType::Struct(fields) => {
            let s: &StructArray = arr.as_struct();
            w.field(103).uvar(fields.len() as u64);
            for (i, (_, ft)) in fields.iter().enumerate() {
                write_vector(w, s.column(i).as_ref(), ft)?;
                w.end();
            }
        }
        DuckType::List(child_t) => match arr.data_type() {
            DataType::List(_) => write_list::<i32>(w, arr.as_list::<i32>(), child_t)?,
            DataType::LargeList(_) => write_list::<i64>(w, arr.as_list::<i64>(), child_t)?,
            other => bail!("quack: unexpected list array type {other}"),
        },
        other => bail!("quack: no encoder for {other:?}"),
    }
    Ok(())
}

fn write_list<O: OffsetSizeTrait>(w: &mut Writer, l: &GenericListArray<O>, child_t: &DuckType) -> Result<()> {
    let offsets = l.value_offsets();
    let base = offsets[0].as_usize();
    let end = offsets[l.len()].as_usize();
    let child = l.values().slice(base, end - base);
    w.field(104).uvar((end - base) as u64); // list_size
    w.field(105).uvar(l.len() as u64);
    for i in 0..l.len() {
        let (off, len) = if l.is_null(i) {
            (0, 0)
        } else {
            (offsets[i].as_usize() - base, offsets[i + 1].as_usize() - offsets[i].as_usize())
        };
        w.field(100).uvar(off as u64);
        w.field(101).uvar(len as u64);
        w.end();
    }
    w.field(106);
    write_vector(w, child.as_ref(), child_t)?;
    w.end();
    Ok(())
}

fn fixed_bytes(arr: &dyn Array, t: &DuckType, out: &mut Vec<u8>) -> Result<()> {
    macro_rules! prim {
        ($ty:ty) => {{
            for v in arr.as_primitive::<$ty>().values().iter() {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }};
    }
    match t {
        DuckType::Boolean => {
            let b: &BooleanArray = arr.as_boolean();
            out.extend((0..b.len()).map(|i| (b.is_valid(i) && b.value(i)) as u8));
        }
        DuckType::TinyInt => prim!(Int8Type),
        DuckType::SmallInt => prim!(Int16Type),
        DuckType::Integer => prim!(Int32Type),
        DuckType::BigInt => prim!(Int64Type),
        DuckType::UTinyInt => prim!(UInt8Type),
        DuckType::USmallInt => prim!(UInt16Type),
        DuckType::UInteger => prim!(UInt32Type),
        DuckType::UBigInt => prim!(UInt64Type),
        DuckType::Float => prim!(Float32Type),
        DuckType::Double => prim!(Float64Type),
        DuckType::Date => prim!(Date32Type),
        DuckType::Time => prim!(Time64MicrosecondType),
        DuckType::Timestamp | DuckType::TimestampTz => prim!(TimestampMicrosecondType),
        DuckType::Decimal(p, _) => {
            let a = arr.as_primitive::<Decimal128Type>();
            let width = decimal_width(*p);
            for v in a.values().iter() {
                let bytes = v.to_le_bytes(); // i128 LE == hugeint_t {lower, upper}
                out.extend_from_slice(&bytes[..width]);
            }
        }
        DuckType::Interval => {
            let a = arr.as_primitive::<IntervalMonthDayNanoType>();
            for v in a.values().iter() {
                out.extend_from_slice(&v.months.to_le_bytes());
                out.extend_from_slice(&v.days.to_le_bytes());
                out.extend_from_slice(&(v.nanoseconds / 1000).to_le_bytes());
            }
        }
        other => bail!("quack: {other:?} is not fixed-width"),
    }
    Ok(())
}

/// Encode a batch as a sequence of DataChunks (each ≤ VECTOR_SIZE rows). Each
/// returned buffer holds the chunk's fields + terminator (one serialized object).
pub fn encode_chunks(batch: &RecordBatch, types: &[DuckType]) -> Result<Vec<(Vec<u8>, usize)>> {
    let cols = canonicalize(batch, types)?;
    let mut out = vec![];
    let mut start = 0;
    while start < batch.num_rows() {
        let len = (batch.num_rows() - start).min(VECTOR_SIZE);
        let slice: Vec<ArrayRef> = cols.iter().map(|c| c.slice(start, len)).collect();
        let mut w = Writer::new();
        write_chunk(&mut w, &slice, types)?;
        w.end();
        out.push((w.buf, len));
        start += len;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array, ListArray, StringArray};

    #[test]
    fn logical_type_bytes() {
        let mut w = Writer::new();
        DuckType::Integer.write(&mut w);
        assert_eq!(w.buf, vec![0x64, 0x00, 13, 0xFF, 0xFF]);

        let mut w = Writer::new();
        DuckType::Decimal(10, 2).write(&mut w);
        assert_eq!(
            w.buf,
            vec![
                0x64, 0x00, 21, // id DECIMAL
                0x65, 0x00, 0x01, // type_info present
                0x64, 0x00, 2, // DECIMAL_TYPE_INFO
                0xC8, 0x00, 10, // width
                0xC9, 0x00, 2, // scale
                0xFF, 0xFF, // end type info
                0xFF, 0xFF, // end logical type
            ]
        );

        let mut w = Writer::new();
        DuckType::List(Box::new(DuckType::Varchar)).write(&mut w);
        assert_eq!(
            w.buf,
            vec![0x64, 0x00, 101, 0x65, 0x00, 1, 0x64, 0x00, 4, 0xC8, 0x00, 0x64, 0x00, 25, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
    }

    #[test]
    fn chunk_bytes_integer_and_varchar_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![Some("ab"), None])),
            ],
        )
        .unwrap();
        let types = duck_types(&batch.schema());
        let chunks = encode_chunks(&batch, &types).unwrap();
        assert_eq!(chunks.len(), 1);
        let mut expect = vec![
            0x64, 0x00, 2, // rows = 2
            0x65, 0x00, 2, // types: 2
            0x64, 0x00, 13, 0xFF, 0xFF, // INTEGER
            0x64, 0x00, 25, 0xFF, 0xFF, // VARCHAR
            0x66, 0x00, 2, // columns: 2
            // col 0: has_validity_mask, validity (8 bytes, bit0 set), data (8 bytes)
            0x64, 0x00, 1, 0x65, 0x00, 8, 1, 0, 0, 0, 0, 0, 0, 0,
            0x66, 0x00, 8, 1, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF,
            // col 1: validity, byte_data_length=2, lengths [2,0], bytes "ab"
            0x64, 0x00, 1, 0x65, 0x00, 8, 1, 0, 0, 0, 0, 0, 0, 0,
            0x6B, 0x00, 2,
            0x6C, 0x00, 8, 2, 0, 0, 0, 0, 0, 0, 0,
            0x6D, 0x00, 2, b'a', b'b',
            0xFF, 0xFF,
            0xFF, 0xFF, // end chunk
        ];
        assert_eq!(chunks[0].0, std::mem::take(&mut expect));
    }

    #[test]
    fn list_vector_layout() {
        let l = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1), Some(2)]), None, Some(vec![])]);
        let t = duck_type(l.data_type());
        assert_eq!(t, DuckType::List(Box::new(DuckType::Integer)));
        let mut w = Writer::new();
        write_vector(&mut w, &l, &t).unwrap();
        let b = &w.buf;
        // has_validity_mask=true, mask word 0b101
        assert_eq!(&b[..14], &[0x64, 0x00, 1, 0x65, 0x00, 8, 0b101, 0, 0, 0, 0, 0, 0, 0]);
        // list_size = 2, entries = 3
        assert_eq!(&b[14..20], &[0x68, 0x00, 2, 0x69, 0x00, 3]);
    }

    #[test]
    fn chunking_respects_vector_size() {
        let n = VECTOR_SIZE * 2 + 5;
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..n as i64))]).unwrap();
        let types = duck_types(&batch.schema());
        let chunks = encode_chunks(&batch, &types).unwrap();
        assert_eq!(chunks.iter().map(|c| c.1).collect::<Vec<_>>(), vec![VECTOR_SIZE, VECTOR_SIZE, 5]);
    }

    #[test]
    fn ddl() {
        let s = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amt", DataType::Decimal128(12, 2), true),
            Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), true),
        ]);
        assert_eq!(
            create_table_sql("orders", &s),
            "CREATE TABLE \"orders\"(\"id\" BIGINT, \"amt\" DECIMAL(12,2), \"tags\" VARCHAR[], \"ts\" TIMESTAMPTZ);"
        );
    }
}
