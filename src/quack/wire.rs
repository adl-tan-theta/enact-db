//! DuckDB `BinarySerializer` encoding as used by the Quack protocol (v3).
//!
//! Derived from duckdb/src/common/serializer/binary_serializer.cpp and
//! duckdb-quack/src/serialize_quack_message.cpp:
//! * a property is a little-endian `u16` field id followed by its value
//! * an object ends with field id `0xFFFF`
//! * integers are LEB128 (signed types use signed LEB128), bool is one raw byte,
//!   float/double are raw little-endian
//! * strings and byte blobs are `varint(len)` + bytes; lists are `varint(count)` + items
//! * nullable pointers are a presence byte followed by the object
//! * `WritePropertyWithDefault` omits the property when it equals its default
//! * `optional_idx` unset is written as `u64::MAX`
//! * `hugeint` is `signed varint(upper)` + `unsigned varint(lower)`

use anyhow::{anyhow, bail, Result};

pub const TERMINATOR: u16 = 0xFFFF;
pub const INVALID_INDEX: u64 = u64::MAX;

pub mod msg {
    pub const CONNECTION_REQUEST: u8 = 1;
    pub const CONNECTION_RESPONSE: u8 = 2;
    pub const PREPARE_REQUEST: u8 = 3;
    pub const PREPARE_RESPONSE: u8 = 4;
    pub const FETCH_REQUEST: u8 = 7;
    pub const FETCH_RESPONSE: u8 = 8;
    pub const SEND_DATA_REQUEST: u8 = 9;
    pub const SUCCESS_RESPONSE: u8 = 10;
    pub const DISCONNECT_MESSAGE: u8 = 11;
    pub const CANCEL_REQUEST: u8 = 12;
    pub const ACKNOWLEDGEMENT: u8 = 15;
    pub const HEARTBEAT_REQUEST: u8 = 16;
    pub const ERROR_RESPONSE: u8 = 100;
}

pub const QUACK_VERSION: u64 = 3;

#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
}

#[allow(dead_code)]
impl Writer {
    pub fn new() -> Self {
        Self { buf: Vec::with_capacity(4096) }
    }
    pub fn field(&mut self, id: u16) -> &mut Self {
        self.buf.extend_from_slice(&id.to_le_bytes());
        self
    }
    pub fn end(&mut self) -> &mut Self {
        self.field(TERMINATOR)
    }
    pub fn uvar(&mut self, mut v: u64) -> &mut Self {
        loop {
            let mut byte = (v & 0x7F) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            self.buf.push(byte);
            if v == 0 {
                break;
            }
        }
        self
    }
    pub fn ivar(&mut self, mut v: i64) -> &mut Self {
        loop {
            let byte = (v & 0x7F) as u8;
            v >>= 7;
            let done = (v == 0 && byte & 0x40 == 0) || (v == -1 && byte & 0x40 != 0);
            self.buf.push(if done { byte } else { byte | 0x80 });
            if done {
                break;
            }
        }
        self
    }
    pub fn boolean(&mut self, b: bool) -> &mut Self {
        self.buf.push(b as u8);
        self
    }
    pub fn string(&mut self, s: &str) -> &mut Self {
        self.uvar(s.len() as u64);
        self.buf.extend_from_slice(s.as_bytes());
        self
    }
    /// `WriteDataPtr`: varint length + raw bytes.
    pub fn blob(&mut self, b: &[u8]) -> &mut Self {
        self.uvar(b.len() as u64);
        self.buf.extend_from_slice(b);
        self
    }
    pub fn hugeint(&mut self, upper: i64, lower: u64) -> &mut Self {
        self.ivar(upper).uvar(lower)
    }

    // ---- property helpers --------------------------------------------------------
    pub fn prop_uvar(&mut self, id: u16, v: u64) -> &mut Self {
        self.field(id).uvar(v)
    }
    pub fn prop_uvar_default(&mut self, id: u16, v: u64, default: u64) -> &mut Self {
        if v != default {
            self.field(id).uvar(v);
        }
        self
    }
    pub fn prop_string_default(&mut self, id: u16, s: &str) -> &mut Self {
        if !s.is_empty() {
            self.field(id).string(s);
        }
        self
    }
    pub fn prop_bool_default(&mut self, id: u16, b: bool) -> &mut Self {
        if b {
            self.field(id).boolean(true);
        }
        self
    }
    /// `optional_idx` with `WritePropertyWithDefault` (omitted when unset).
    pub fn prop_optional_idx_default(&mut self, id: u16, v: Option<u64>) -> &mut Self {
        if let Some(v) = v {
            self.field(id).uvar(v);
        }
        self
    }
}

/// Message header object (`MessageHeader::Serialize`).
pub fn write_header(w: &mut Writer, msg_type: u8, connection_id: &str, client_query_id: Option<u64>) {
    w.field(1).uvar(msg_type as u64);
    w.prop_string_default(2, connection_id);
    w.field(3).uvar(client_query_id.unwrap_or(INVALID_INDEX));
    w.end();
}

pub struct Reader<'a> {
    b: &'a [u8],
    pub pos: usize,
}

#[allow(dead_code)]
impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Self { b, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.b.len() {
            bail!("quack: unexpected end of message");
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    pub fn field(&mut self) -> Result<u16> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }
    pub fn u8raw(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn uvar(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0;
        loop {
            let byte = self.u8raw()?;
            if shift >= 64 {
                bail!("quack: varint overflow");
            }
            result |= ((byte & 0x7F) as u64) << shift;
            shift += 7;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
        }
    }
    pub fn ivar(&mut self) -> Result<i64> {
        let mut result: i64 = 0;
        let mut shift = 0;
        loop {
            let byte = self.u8raw()?;
            if shift >= 64 {
                bail!("quack: varint overflow");
            }
            result |= ((byte & 0x7F) as i64) << shift;
            shift += 7;
            if byte & 0x80 == 0 {
                if shift < 64 && byte & 0x40 != 0 {
                    result |= -1i64 << shift;
                }
                return Ok(result);
            }
        }
    }
    pub fn boolean(&mut self) -> Result<bool> {
        Ok(self.u8raw()? != 0)
    }
    pub fn string(&mut self) -> Result<String> {
        let n = self.uvar()? as usize;
        let s = self.take(n)?;
        String::from_utf8(s.to_vec()).map_err(|_| anyhow!("quack: invalid utf-8 string"))
    }
    pub fn blob(&mut self) -> Result<&'a [u8]> {
        let n = self.uvar()? as usize;
        self.take(n)
    }
    pub fn hugeint(&mut self) -> Result<(i64, u64)> {
        Ok((self.ivar()?, self.uvar()?))
    }
    pub fn remaining(&self) -> usize {
        self.b.len() - self.pos
    }
}

#[derive(Debug, Clone)]
pub struct Header {
    pub msg_type: u8,
    pub connection_id: String,
    pub client_query_id: Option<u64>,
}

pub fn read_header(r: &mut Reader) -> Result<Header> {
    let mut h = Header { msg_type: 0, connection_id: String::new(), client_query_id: None };
    loop {
        match r.field()? {
            TERMINATOR => break,
            1 => h.msg_type = r.uvar()? as u8,
            2 => h.connection_id = r.string()?,
            3 => {
                let v = r.uvar()?;
                h.client_query_id = (v != INVALID_INDEX).then_some(v);
            }
            f => bail!("quack: unknown header field {f}"),
        }
    }
    Ok(h)
}

pub type Uuid = (i64, u64);

#[derive(Debug)]
#[allow(dead_code)]
pub enum Request {
    Connect { auth: String, client_version: String, min_version: u64, max_version: u64, heartbeat: u64 },
    Prepare { sql: String, uuid: Uuid, inline_rows: Option<u64> },
    Fetch { uuid: Uuid, batch_index: u64, ack_index: u64 },
    Disconnect,
    Heartbeat,
    Cancel { uuid: Uuid },
    Ack { uuid: Uuid },
    SendData,
}

/// Decode one request message body (after the header).
pub fn read_request(h: &Header, r: &mut Reader) -> Result<Request> {
    macro_rules! fields {
        ($($id:literal => $body:expr),* $(,)?) => {
            loop {
                match r.field()? {
                    TERMINATOR => break,
                    $($id => { $body; })*
                    f => bail!("quack: unknown field {f} in message type {}", h.msg_type),
                }
            }
        };
    }
    Ok(match h.msg_type {
        msg::CONNECTION_REQUEST => {
            let (mut auth, mut client_version, mut min_version, mut max_version, mut heartbeat) =
                (String::new(), String::new(), 0, 0, 0);
            fields! {
                1 => auth = r.string()?,
                2 => client_version = r.string()?,
                3 => { r.string()?; },
                4 => min_version = r.uvar()?,
                5 => max_version = r.uvar()?,
                6 => { r.string()?; },
                7 => heartbeat = r.uvar()?,
            }
            Request::Connect { auth, client_version, min_version, max_version, heartbeat }
        }
        msg::PREPARE_REQUEST => {
            let (mut sql, mut uuid, mut inline_rows) = (String::new(), (0, 0), None);
            fields! {
                1 => sql = r.string()?,
                2 => uuid = r.hugeint()?,
                3 => { let v = r.uvar()?; inline_rows = (v != INVALID_INDEX).then_some(v); },
            }
            Request::Prepare { sql, uuid, inline_rows }
        }
        msg::FETCH_REQUEST => {
            let (mut uuid, mut batch_index, mut ack_index) = ((0, 0), 0, 0);
            fields! {
                1 => uuid = r.hugeint()?,
                2 => batch_index = r.uvar()?,
                3 => ack_index = r.uvar()?,
            }
            Request::Fetch { uuid, batch_index, ack_index }
        }
        msg::DISCONNECT_MESSAGE => {
            fields! {}
            Request::Disconnect
        }
        msg::HEARTBEAT_REQUEST => {
            fields! {}
            Request::Heartbeat
        }
        msg::CANCEL_REQUEST => {
            let mut uuid = (0, 0);
            fields! { 1 => uuid = r.hugeint()? }
            Request::Cancel { uuid }
        }
        msg::ACKNOWLEDGEMENT => {
            let mut uuid = (0, 0);
            fields! { 1 => uuid = r.hugeint()? }
            Request::Ack { uuid }
        }
        msg::SEND_DATA_REQUEST => Request::SendData,
        t => bail!("quack: unsupported message type {t}"),
    })
}

// ---- responses ---------------------------------------------------------------------

pub fn connection_response(connection_id: &str, heartbeat: u64) -> Vec<u8> {
    let mut w = Writer::new();
    write_header(&mut w, msg::CONNECTION_RESPONSE, connection_id, None);
    w.prop_string_default(1, concat!("enact-db-", env!("CARGO_PKG_VERSION")));
    w.prop_string_default(2, platform());
    w.prop_uvar_default(3, QUACK_VERSION, 0);
    w.prop_uvar_default(4, heartbeat, 0);
    w.end();
    w.buf
}

pub fn success_response() -> Vec<u8> {
    let mut w = Writer::new();
    write_header(&mut w, msg::SUCCESS_RESPONSE, "", None);
    w.end();
    w.buf
}

/// `ErrorResponse`: message, exception type name as DuckDB spells it (e.g. "Catalog").
pub fn error_response(message: &str, exception_type: &str) -> Vec<u8> {
    let mut w = Writer::new();
    write_header(&mut w, msg::ERROR_RESPONSE, "", None);
    w.prop_string_default(1, message);
    w.prop_string_default(2, exception_type);
    w.end();
    w.buf
}

fn platform() -> &'static str {
    if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        "linux_amd64"
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        "linux_arm64"
    } else if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        "osx_arm64"
    } else if cfg!(target_os = "macos") {
        "osx_amd64"
    } else if cfg!(target_os = "windows") {
        "windows_amd64"
    } else {
        "unknown"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leb128_matches_duckdb() {
        let enc = |f: &dyn Fn(&mut Writer)| {
            let mut w = Writer::new();
            f(&mut w);
            w.buf
        };
        assert_eq!(enc(&|w| { w.uvar(0); }), vec![0x00]);
        assert_eq!(enc(&|w| { w.uvar(127); }), vec![0x7F]);
        assert_eq!(enc(&|w| { w.uvar(128); }), vec![0x80, 0x01]);
        assert_eq!(enc(&|w| { w.uvar(624485); }), vec![0xE5, 0x8E, 0x26]);
        assert_eq!(enc(&|w| { w.ivar(-1); }), vec![0x7F]);
        assert_eq!(enc(&|w| { w.ivar(63); }), vec![0x3F]);
        assert_eq!(enc(&|w| { w.ivar(64); }), vec![0xC0, 0x00]);
        assert_eq!(enc(&|w| { w.ivar(-123456); }), vec![0xC0, 0xBB, 0x78]);
        assert_eq!(enc(&|w| { w.uvar(u64::MAX); }).len(), 10);
        for v in [0i64, 1, -1, 63, 64, -64, -65, i64::MAX, i64::MIN, 123456789] {
            let b = enc(&|w| { w.ivar(v); });
            assert_eq!(Reader::new(&b).ivar().unwrap(), v);
        }
    }

    #[test]
    fn header_layout() {
        // MessageHeader{type=PREPARE_RESPONSE, connection_id="", client_query_id=unset}
        let mut w = Writer::new();
        write_header(&mut w, msg::PREPARE_RESPONSE, "", None);
        let mut expect = vec![0x01, 0x00, 0x04, 0x03, 0x00];
        expect.extend([0xFF; 9]);
        expect.push(0x01);
        expect.extend([0xFF, 0xFF]);
        assert_eq!(w.buf, expect);
        let h = read_header(&mut Reader::new(&w.buf)).unwrap();
        assert_eq!(h.msg_type, msg::PREPARE_RESPONSE);
        assert_eq!(h.client_query_id, None);
    }

    #[test]
    fn prepare_request_roundtrip() {
        // Built exactly as PrepareRequestMessage::Serialize would (with a header).
        let mut w = Writer::new();
        write_header(&mut w, msg::PREPARE_REQUEST, "conn-1", Some(7));
        w.field(1).string("SELECT 1");
        w.field(2).hugeint(-5, 42);
        w.end();
        let mut r = Reader::new(&w.buf);
        let h = read_header(&mut r).unwrap();
        assert_eq!(h.connection_id, "conn-1");
        assert_eq!(h.client_query_id, Some(7));
        match read_request(&h, &mut r).unwrap() {
            Request::Prepare { sql, uuid, inline_rows } => {
                assert_eq!(sql, "SELECT 1");
                assert_eq!(uuid, (-5, 42));
                assert_eq!(inline_rows, None);
            }
            other => panic!("{other:?}"),
        }
    }
}
