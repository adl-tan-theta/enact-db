//! Gateway configuration (TOML).
//!
//! ```toml
//! [server]
//! listen   = "127.0.0.1:9494"
//! data_dir = "./data"
//! # Optional bearer token; when set, every /v1 and /ws request must send it.
//! # auth_token = "change-me"
//!
//! [[sources]]
//! name = "orders"
//! kind = "postgres"
//! connection = "host=localhost user=app password=secret dbname=shop"
//! table = "public.orders"
//! ```

use anyhow::{Context, Result};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub crdt: CrdtConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Allow DDL/DML through /v1/query (CREATE VIEW, INSERT into writable providers, ...).
    #[serde(default)]
    pub allow_sql_writes: bool,
    /// Hard cap on rows returned as JSON from /v1/query.
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            data_dir: default_data_dir(),
            auth_token: None,
            allow_sql_writes: false,
            max_rows: default_max_rows(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CrdtConfig {
    /// Compact a document's update log into one snapshot after this many updates.
    #[serde(default = "default_compact_after")]
    pub compact_after: u64,
}

impl Default for CrdtConfig {
    fn default() -> Self {
        Self { compact_after: default_compact_after() }
    }
}

fn default_listen() -> SocketAddr {
    "127.0.0.1:9494".parse().unwrap()
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}
fn default_max_rows() -> usize {
    10_000
}
fn default_compact_after() -> u64 {
    500
}

/// One federated data source, registered as a DataFusion table named `name`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceConfig {
    /// Local or object-store Parquet (file, directory or prefix). `az://` URLs need an [azure] store.
    Parquet { name: String, location: String, #[serde(default)] azure: Option<AzureConfig> },
    /// Local or object-store CSV with a header row.
    Csv { name: String, location: String, #[serde(default)] azure: Option<AzureConfig> },
    /// Local or object-store newline-delimited JSON.
    Json { name: String, location: String, #[serde(default)] azure: Option<AzureConfig> },
    /// A Postgres table (read via datafusion-table-providers, filters/limits pushed down).
    Postgres { name: String, connection: String, table: String },
    /// A Snowflake table through an ADBC driver (build with `--features snowflake`).
    Snowflake {
        name: String,
        /// Path to the ADBC Snowflake driver shared library (libadbc_driver_snowflake.so).
        driver: String,
        /// ADBC URI, e.g. "user:pass@account/db/schema?warehouse=WH".
        uri: String,
        table: String,
    },
}

impl SourceConfig {
    pub fn name(&self) -> &str {
        match self {
            SourceConfig::Parquet { name, .. }
            | SourceConfig::Csv { name, .. }
            | SourceConfig::Json { name, .. }
            | SourceConfig::Postgres { name, .. }
            | SourceConfig::Snowflake { name, .. } => name,
        }
    }
}

/// Azure Blob / ADLS Gen2 credentials. Any field left empty falls back to the
/// standard `AZURE_*` environment variables (via `MicrosoftAzureBuilder::from_env`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AzureConfig {
    pub account: Option<String>,
    pub container: Option<String>,
    /// Name of an env var holding the access key (keeps secrets out of the config file).
    pub access_key_env: Option<String>,
    /// Use Azurite / emulator.
    #[serde(default)]
    pub use_emulator: bool,
}

impl Config {
    pub fn load(path: Option<&std::path::Path>) -> Result<Self> {
        match path {
            Some(p) => {
                let text = std::fs::read_to_string(p)
                    .with_context(|| format!("reading config {}", p.display()))?;
                toml::from_str(&text).with_context(|| format!("parsing config {}", p.display()))
            }
            None => Ok(Config {
                server: ServerConfig::default(),
                sources: vec![],
                crdt: CrdtConfig::default(),
            }),
        }
    }
}
