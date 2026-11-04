//! Quack protocol conformance test against the real binary.
//!
//! The decoder below is an independent re-implementation of DuckDB's
//! `BinaryDeserializer`, `LogicalType::Deserialize`, `DataChunk::Deserialize`
//! and `Vector::Deserialize` (V2_0_0 format), written from the C++ sources,
//! not from enact-db's encoder. Requests are built exactly as the
//! duckdb-quack client builds them, including the catalog queries it sends at ATTACH.

use serde_json::json;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

// ---- minimal BinarySerializer writer/reader ------------------------------------------

struct W(Vec<u8>);
impl W {
    fn f(&mut self, id: u16) -> &mut Self {
        self.0.extend_from_slice(&id.to_le_bytes());
        self
    }
    fn end(&mut self) -> &mut Self {
        self.f(0xFFFF)
    }
    fn u(&mut self, mut v: u64) -> &mut Self {
        loop {
            let mut b = (v & 0x7F) as u8;
            v >>= 7;
            if v != 0 {
                b |= 0x80;
            }
            self.0.push(b);
            if v == 0 {
                return self;
            }
        }
    }
    fn i(&mut self, mut v: i64) -> &mut Self {
        loop {
            let b = (v & 0x7F) as u8;
            v >>= 7;
            let done = (v == 0 && b & 0x40 == 0) || (v == -1 && b & 0x40 != 0);
            self.0.push(if done { b } else { b | 0x80 });
            if done {
                return self;
            }
        }
    }
    fn s(&mut self, s: &str) -> &mut Self {
        self.u(s.len() as u64);
        self.0.extend_from_slice(s.as_bytes());
        self
    }
    fn header(&mut self, t: u8, conn: &str) -> &mut Self {
        self.f(1).u(t as u64);
        if !conn.is_empty() {
            self.f(2).s(conn);
        }
        self.f(3).u(u64::MAX).end()
    }
}

struct R<'a>(&'a [u8], usize);
impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let s = &self.0[self.1..self.1 + n];
        self.1 += n;
        s
    }
    fn f(&mut self) -> u16 {
        let b = self.take(2);
        u16::from_le_bytes([b[0], b[1]])
    }
    fn peek_f(&self) -> u16 {
        u16::from_le_bytes([self.0[self.1], self.0[self.1 + 1]])
    }
    fn u(&mut self) -> u64 {
        let (mut r, mut sh) = (0u64, 0);
        loop {
            let b = self.take(1)[0];
            r |= ((b & 0x7F) as u64) << sh;
            sh += 7;
            if b & 0x80 == 0 {
                return r;
            }
        }
    }
    fn i(&mut self) -> i64 {
        let (mut r, mut sh) = (0i64, 0);
        loop {
            let b = self.take(1)[0];
            r |= ((b & 0x7F) as i64) << sh;
            sh += 7;
            if b & 0x80 == 0 {
                if sh < 64 && b & 0x40 != 0 {
                    r |= -1i64 << sh;
                }
                return r;
            }
        }
    }
    fn s(&mut self) -> String {
        let n = self.u() as usize;
        String::from_utf8(self.take(n).to_vec()).unwrap()
    }
    fn blob(&mut self) -> &'a [u8] {
        let n = self.u() as usize;
        self.take(n)
    }
    fn expect_end(&mut self) {
        assert_eq!(self.f(), 0xFFFF, "expected object terminator at {}", self.1);
    }
    /// Read a property with default: returns true if field `id` is next (and consumes the id).
    fn opt(&mut self, id: u16) -> bool {
        if self.peek_f() == id {
            self.f();
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum T {
    Bool,
    I32,
    I64,
    F64,
    Varchar,
    Decimal(u8, u8),
    Timestamp,
    List(Box<T>),
}

fn read_type(r: &mut R) -> T {
    assert_eq!(r.f(), 100);
    let id = r.u();
    let mut info: Option<(u64, Vec<(u16, T)>, (u64, u64))> = None;
    if r.opt(101) {
        assert_eq!(r.take(1)[0], 1, "nullable type_info present");
        assert_eq!(r.f(), 100);
        let kind = r.u();
        let mut children = vec![];
        let (mut w, mut s) = (0, 0);
        loop {
            match r.f() {
                0xFFFF => break,
                200 if kind == 2 => w = r.u(),
                201 if kind == 2 => s = r.u(),
                200 if kind == 4 => children.push((200, read_type(r))),
                f => panic!("unexpected type info field {f}"),
            }
        }
        info = Some((kind, children, (w, s)));
    }
    r.expect_end();
    match id {
        10 => T::Bool,
        13 => T::I32,
        14 => T::I64,
        23 => T::F64,
        25 => T::Varchar,
        19 => T::Timestamp,
        21 => {
            let (_, _, (w, s)) = info.unwrap();
            T::Decimal(w as u8, s as u8)
        }
        101 => T::List(Box::new(info.unwrap().1.remove(0).1)),
        other => panic!("type id {other} not handled by test decoder"),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum V {
    Null,
    Bool(bool),
    Int(i64),
    F(f64),
    Dec(i128, u8),
    Str(String),
    List(Vec<V>),
}

fn width(t: &T) -> Option<usize> {
    match t {
        T::Bool => Some(1),
        T::I32 => Some(4),
        T::I64 | T::F64 | T::Timestamp => Some(8),
        T::Decimal(w, _) => Some(match w {
            0..=4 => 2,
            5..=9 => 4,
            10..=18 => 8,
            _ => 16,
        }),
        _ => None,
    }
}

/// `Vector::Deserialize` (flat, V2_0_0) → values; consumes the vector object's terminator.
fn read_vector(r: &mut R, t: &T, count: usize) -> Vec<V> {
    assert_eq!(r.f(), 100);
    let has_mask = r.take(1)[0] != 0;
    let mut valid = vec![true; count];
    if has_mask {
        assert_eq!(r.f(), 101);
        let m = r.blob();
        assert_eq!(m.len(), count.div_ceil(64) * 8);
        for (i, v) in valid.iter_mut().enumerate() {
            let word = u64::from_le_bytes(m[i / 64 * 8..i / 64 * 8 + 8].try_into().unwrap());
            *v = word & (1 << (i % 64)) != 0;
        }
    }
    let vals: Vec<V> = if let Some(wd) = width(t) {
        assert_eq!(r.f(), 102);
        let data = r.blob();
        assert_eq!(data.len(), wd * count);
        (0..count)
            .map(|i| {
                let b = &data[i * wd..(i + 1) * wd];
                match t {
                    T::Bool => V::Bool(b[0] != 0),
                    T::I32 => V::Int(i32::from_le_bytes(b.try_into().unwrap()) as i64),
                    T::I64 | T::Timestamp => V::Int(i64::from_le_bytes(b.try_into().unwrap())),
                    T::F64 => V::F(f64::from_le_bytes(b.try_into().unwrap())),
                    T::Decimal(_, s) => {
                        let mut full = [0u8; 16];
                        full[..wd].copy_from_slice(b);
                        if b[wd - 1] & 0x80 != 0 {
                            full[wd..].fill(0xFF);
                        }
                        V::Dec(i128::from_le_bytes(full), *s)
                    }
                    _ => unreachable!(),
                }
            })
            .collect()
    } else {
        match t {
            T::Varchar => {
                assert_eq!(r.f(), 107);
                let total = r.u() as usize;
                assert_eq!(r.f(), 108);
                let lens = r.blob();
                assert_eq!(r.f(), 109);
                let bytes = r.blob();
                assert_eq!(bytes.len(), total);
                let mut off = 0;
                (0..count)
                    .map(|i| {
                        let l = u32::from_le_bytes(lens[i * 4..i * 4 + 4].try_into().unwrap()) as usize;
                        let s = String::from_utf8(bytes[off..off + l].to_vec()).unwrap();
                        off += l;
                        V::Str(s)
                    })
                    .collect()
            }
            T::List(child) => {
                assert_eq!(r.f(), 104);
                let list_size = r.u() as usize;
                assert_eq!(r.f(), 105);
                let n = r.u() as usize;
                assert_eq!(n, count);
                let mut entries = vec![];
                for _ in 0..n {
                    assert_eq!(r.f(), 100);
                    let off = r.u() as usize;
                    assert_eq!(r.f(), 101);
                    let len = r.u() as usize;
                    r.expect_end();
                    entries.push((off, len));
                }
                assert_eq!(r.f(), 106);
                let child_vals = read_vector(r, child, list_size);
                entries.iter().map(|(o, l)| V::List(child_vals[*o..*o + *l].to_vec())).collect()
            }
            _ => unreachable!(),
        }
    };
    r.expect_end();
    vals.into_iter().zip(valid).map(|(v, ok)| if ok { v } else { V::Null }).collect()
}

/// `DataChunk::Deserialize` fields (caller handles the surrounding object).
fn read_chunk(r: &mut R) -> Vec<Vec<V>> {
    assert_eq!(r.f(), 100);
    let rows = r.u() as usize;
    assert!(rows >= 1 && rows <= 2048, "chunk rows {rows}");
    assert_eq!(r.f(), 101);
    let ntypes = r.u();
    let types: Vec<T> = (0..ntypes).map(|_| read_type(r)).collect();
    assert_eq!(r.f(), 102);
    assert_eq!(r.u(), ntypes);
    let cols: Vec<Vec<V>> = types.iter().map(|t| read_vector(r, t, rows)).collect();
    (0..rows).map(|i| cols.iter().map(|c| c[i].clone()).collect()).collect()
}

#[derive(Debug)]
enum Resp {
    Connected { conn: String, version: u64 },
    Prepared { types: Vec<T>, names: Vec<String>, more: bool, rows: Vec<Vec<V>>, uuid: (i64, u64) },
    Fetched { rows: Vec<Vec<V>>, chunks: u64, batch: Option<u64>, total: Option<u64>, raw: Vec<u8> },
    Success,
    Error { message: String, kind: String },
}

fn decode(bytes: &[u8]) -> Resp {
    let mut r = R(bytes, 0);
    assert_eq!(r.f(), 1);
    let t = r.u();
    let mut conn = String::new();
    if r.opt(2) {
        conn = r.s();
    }
    assert_eq!(r.f(), 3);
    r.u();
    r.expect_end();
    let resp = match t {
        2 => {
            let mut version = 0;
            loop {
                match r.f() {
                    0xFFFF => break,
                    1 | 2 => {
                        r.s();
                    }
                    3 => version = r.u(),
                    4 => {
                        r.u();
                    }
                    f => panic!("conn resp field {f}"),
                }
            }
            Resp::Connected { conn, version }
        }
        4 => {
            let (mut types, mut names, mut more, mut rows, mut uuid) = (vec![], vec![], false, vec![], (0, 0));
            loop {
                match r.f() {
                    0xFFFF => break,
                    1 => types = (0..r.u()).map(|_| read_type(&mut r)).collect(),
                    2 => names = (0..r.u()).map(|_| r.s()).collect(),
                    3 => more = r.take(1)[0] != 0,
                    4 => {
                        for _ in 0..r.u() {
                            assert_eq!(r.take(1)[0], 1);
                            assert_eq!(r.f(), 300);
                            rows.extend(read_chunk(&mut r));
                            r.expect_end(); // chunk object
                            r.expect_end(); // wrapper object
                        }
                    }
                    5 => uuid = (r.i(), r.u()),
                    f => panic!("prepare resp field {f}"),
                }
            }
            Resp::Prepared { types, names, more, rows, uuid }
        }
        8 => {
            let (mut chunks, mut total, mut batch) = (0, None, None);
            loop {
                match r.f() {
                    0xFFFF => break,
                    1 => chunks = r.u(),
                    2 => total = Some(r.u()),
                    3 => batch = Some(r.u()),
                    f => panic!("fetch resp field {f}"),
                }
            }
            let mut rows = vec![];
            for _ in 0..chunks {
                rows.extend(read_chunk(&mut r));
                r.expect_end();
            }
            Resp::Fetched { rows, chunks, batch, total, raw: bytes.to_vec() }
        }
        10 => {
            r.expect_end();
            Resp::Success
        }
        100 => {
            let (mut message, mut kind) = (String::new(), String::new());
            loop {
                match r.f() {
                    0xFFFF => break,
                    1 => message = r.s(),
                    2 => kind = r.s(),
                    4 => {
                        r.take(1);
                    }
                    f => panic!("error field {f}"),
                }
            }
            Resp::Error { message, kind }
        }
        other => panic!("unexpected response type {other}"),
    };
    assert_eq!(r.1, bytes.len(), "trailing bytes in response");
    resp
}

// ---- client --------------------------------------------------------------------------

struct Client {
    url: String,
    http: reqwest::Client,
    conn: String,
}

impl Client {
    async fn post(&self, body: Vec<u8>) -> Resp {
        let r = self.http.post(&self.url).header("content-type", "application/vnd.duckdb").body(body).send().await.unwrap();
        assert_eq!(r.status(), 200);
        decode(&r.bytes().await.unwrap())
    }
    async fn connect(url: &str, token: &str) -> (Client, Resp) {
        let mut c = Client { url: url.to_string(), http: reqwest::Client::new(), conn: String::new() };
        let mut w = W(vec![]);
        w.header(1, "");
        w.f(1).s(token).f(2).s("v1.5.0").f(3).s("linux_amd64").f(4).u(3).f(5).u(3).f(6).s("test-client").f(7).u(60).end();
        let resp = c.post(w.0).await;
        if let Resp::Connected { conn, .. } = &resp {
            c.conn = conn.clone();
        }
        (c, resp)
    }
    async fn prepare(&self, sql: &str, uuid: (i64, u64), inline: Option<u64>) -> Resp {
        let mut w = W(vec![]);
        w.header(3, &self.conn);
        w.f(1).s(sql).f(2).i(uuid.0).u(uuid.1);
        if let Some(n) = inline {
            w.f(3).u(n);
        }
        w.end();
        self.post(w.0).await
    }
    async fn fetch(&self, uuid: (i64, u64), batch: u64, ack: u64) -> Resp {
        let mut w = W(vec![]);
        w.header(7, &self.conn);
        w.f(1).i(uuid.0).u(uuid.1);
        if batch != 0 {
            w.f(2).u(batch);
        }
        if ack != 0 {
            w.f(3).u(ack);
        }
        w.end();
        self.post(w.0).await
    }
    async fn simple(&self, t: u8) -> Resp {
        let mut w = W(vec![]);
        w.header(t, &self.conn);
        w.end();
        self.post(w.0).await
    }
}

// The exact catalog queries duckdb-quack sends at ATTACH (storage/quack_schema.cpp, quack_table.cpp).
const SCHEMA_QUERY: &str = r#"
WITH RECURSIVE schema_tree AS (
	SELECT oid, database_name, [schema_name] AS schema_path
	FROM duckdb_schemas()
	WHERE parent_schema_oid IS NULL
	UNION ALL
	SELECT nested.oid, nested.database_name, list_append(parent.schema_path, nested.schema_name)
	FROM duckdb_schemas() nested
	JOIN schema_tree parent ON nested.parent_schema_oid = parent.oid
)
SELECT oid, database_name, schema_path, current_database()
FROM schema_tree
WHERE database_name NOT IN ('system', 'temp')
ORDER BY (database_name = current_database()) DESC, database_name, length(schema_path), schema_path
	"#;
const TABLE_QUERY: &str = r#"
SELECT schema_oid, sql, 'table'
FROM duckdb_tables()
UNION ALL
SELECT schema_oid, view_name, 'view'
FROM duckdb_views()
	"#;

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn quack_protocol() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("customers.csv"),
        "id,name,revenue\n1,Acme,1200.50\n2,Globex,\n3,Initech,88.25\n",
    )
    .unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    std::fs::write(
        dir.path().join("cfg.toml"),
        format!(
            "[server]\nlisten = \"127.0.0.1:{port}\"\ndata_dir = \"{}\"\n[quack]\ntoken = \"duck-secret\"\n\
             [[sources]]\nkind = \"csv\"\nname = \"customers\"\nlocation = \"{}\"\n",
            dir.path().join("data").display(),
            dir.path().join("customers.csv").display()
        ),
    )
    .unwrap();
    let _srv = Server(
        Command::new(env!("CARGO_BIN_EXE_enact-db"))
            .arg("--config")
            .arg(dir.path().join("cfg.toml"))
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let base = format!("http://127.0.0.1:{port}");
    for _ in 0..200 {
        if reqwest::get(format!("{base}/health")).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    reqwest::Client::new()
        .put(format!("{base}/v1/agents/analyst/kv/tags"))
        .json(&json!(["vip", "eu"]))
        .send()
        .await
        .unwrap();
    let url = format!("{base}/quack");

    // auth
    let (_, r) = Client::connect(&url, "wrong").await;
    assert!(matches!(r, Resp::Error { ref message, .. } if message.contains("Authentication")), "{r:?}");
    let (c, r) = Client::connect(&url, "duck-secret").await;
    match r {
        Resp::Connected { version, .. } => assert_eq!(version, 3),
        other => panic!("{other:?}"),
    }

    // ATTACH: schema discovery
    match c.prepare(SCHEMA_QUERY, (0, 0), None).await {
        Resp::Prepared { types, rows, more, .. } => {
            assert_eq!(types, vec![T::I64, T::Varchar, T::List(Box::new(T::Varchar)), T::Varchar]);
            assert!(!more);
            assert_eq!(
                rows,
                vec![
                    vec![V::Int(1), V::Str("gateway".into()), V::List(vec![V::Str("main".into())]), V::Str("gateway".into())],
                    vec![V::Int(2), V::Str("gateway".into()), V::List(vec![V::Str("system".into())]), V::Str("gateway".into())],
                ]
            );
        }
        other => panic!("{other:?}"),
    }
    // ATTACH: tables with DuckDB DDL
    match c.prepare(TABLE_QUERY, (0, 0), None).await {
        Resp::Prepared { rows, .. } => {
            let customers = rows
                .iter()
                .find(|r| matches!(&r[1], V::Str(s) if s.contains("\"customers\"")))
                .expect("customers listed");
            assert_eq!(customers[0], V::Int(1));
            assert_eq!(customers[1], V::Str("CREATE TABLE \"customers\"(\"id\" BIGINT, \"name\" VARCHAR, \"revenue\" DOUBLE);".into()));
            assert_eq!(customers[2], V::Str("table".into()));
            assert!(rows.iter().any(|r| r[0] == V::Int(2) && matches!(&r[1], V::Str(s) if s.contains("\"agent_tool_calls\""))));
        }
        other => panic!("{other:?}"),
    }

    // scan with projection pushdown, nulls preserved
    match c.prepare("SELECT #2, #3 FROM main.customers", (1, 1), None).await {
        Resp::Prepared { names, rows, more, uuid, .. } => {
            assert_eq!(names, vec!["name", "revenue"]);
            assert!(!more);
            assert_eq!(uuid, (1, 1));
            assert_eq!(
                rows,
                vec![
                    vec![V::Str("Acme".into()), V::F(1200.5)],
                    vec![V::Str("Globex".into()), V::Null],
                    vec![V::Str("Initech".into()), V::F(88.25)],
                ]
            );
        }
        other => panic!("{other:?}"),
    }
    // count(*) shape and FROM-first full scan of a system table
    match c.prepare("SELECT NULL::BOOLEAN FROM main.customers", (1, 2), None).await {
        Resp::Prepared { rows, types, .. } => {
            assert_eq!(types, vec![T::Bool]);
            assert_eq!(rows.len(), 3);
        }
        other => panic!("{other:?}"),
    }
    match c.prepare("FROM system.agent_kv", (1, 3), None).await {
        Resp::Prepared { rows, .. } => {
            assert_eq!(rows, vec![vec![V::Str("analyst".into()), V::Str("tags".into()), V::Str("[\"vip\",\"eu\"]".into())]]);
        }
        other => panic!("{other:?}"),
    }
    // quack_query passthrough: arbitrary DataFusion SQL with decimals
    match c.prepare("SELECT CAST(12.34 AS DECIMAL(10,2)) AS d, CAST(-5 AS DECIMAL(4,1)) AS e", (1, 4), None).await {
        Resp::Prepared { types, rows, .. } => {
            assert_eq!(types, vec![T::Decimal(10, 2), T::Decimal(4, 1)]);
            assert_eq!(rows, vec![vec![V::Dec(1234, 2), V::Dec(-50, 1)]]);
        }
        other => panic!("{other:?}"),
    }

    // large result: inline + streamed FETCH batches, retry, fetch-ahead, terminal total
    let uuid = (7, 99);
    let (inline_rows, more) = match c
        .prepare("SELECT value FROM generate_series(1, 100000) AS t(value)", uuid, Some(3000))
        .await
    {
        Resp::Prepared { rows, more, .. } => (rows, more),
        other => panic!("{other:?}"),
    };
    assert!(more);
    assert!(inline_rows.len() >= 3000 && inline_rows.len() < 100000);
    let mut all: Vec<i64> = inline_rows.iter().map(|r| if let V::Int(i) = r[0] { i } else { panic!() }).collect();
    // fetch-ahead: ask for batch 2 before batch 1
    let b2 = c.fetch(uuid, 2, 0).await;
    let b1 = c.fetch(uuid, 1, 0).await;
    let retry1 = c.fetch(uuid, 1, 0).await;
    let (raw1, raw1_retry) = match (&b1, &retry1) {
        (Resp::Fetched { raw: a, .. }, Resp::Fetched { raw: b, .. }) => (a.clone(), b.clone()),
        _ => panic!("{b1:?}"),
    };
    assert_eq!(raw1, raw1_retry, "a retried FETCH returns the same batch");
    let mut next = 1u64;
    for b in [b1, b2] {
        match b {
            Resp::Fetched { rows, batch, chunks, .. } => {
                assert_eq!(batch, Some(next));
                assert!(chunks > 0);
                all.extend(rows.iter().map(|r| if let V::Int(i) = r[0] { i } else { panic!() }));
            }
            other => panic!("{other:?}"),
        }
        next += 1;
    }
    loop {
        match c.fetch(uuid, next, next - 1).await {
            Resp::Fetched { rows, chunks: 0, total, .. } => {
                assert!(rows.is_empty());
                assert_eq!(total, Some(next - 1));
                break;
            }
            Resp::Fetched { rows, batch, .. } => {
                assert_eq!(batch, Some(next));
                all.extend(rows.iter().map(|r| if let V::Int(i) = r[0] { i } else { panic!() }));
                next += 1;
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(all, (1..=100000).collect::<Vec<i64>>());

    // errors keep DuckDB exception kinds; writes are refused
    match c.prepare("SELECT * FROM no_such_table", (2, 1), None).await {
        Resp::Error { kind, message } => assert_eq!(kind, "Catalog", "{message}"),
        other => panic!("{other:?}"),
    }
    match c.prepare("CREATE TABLE x AS SELECT 1", (2, 2), None).await {
        Resp::Error { message, .. } => assert!(message.contains("read-only"), "{message}"),
        other => panic!("{other:?}"),
    }
    match c.fetch((123, 456), 1, 0).await {
        Resp::Error { message, .. } => assert!(message.contains("closed")),
        other => panic!("{other:?}"),
    }

    // heartbeat, disconnect, then the connection is gone
    assert!(matches!(c.simple(16).await, Resp::Success));
    assert!(matches!(c.simple(11).await, Resp::Success));
    match c.prepare("SELECT 1", (3, 1), None).await {
        Resp::Error { message, .. } => assert!(message.contains("Invalid connection id")),
        other => panic!("{other:?}"),
    }
}
