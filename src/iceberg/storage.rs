//! iceberg-rust `Storage` implemented on `object_store`, so Iceberg data uses
//! the same storage stack (and Azure/S3 credentials) as the rest of enact-db.
//!
//! Paths are full URLs (`s3://bucket/key`, `abfss://container@account.dfs.core.windows.net/key`,
//! `az://container/key`, `file:///path`). One `ObjectStore` is built per
//! scheme+authority and cached. Credentials come from, in order: properties the
//! REST catalog vends for the table (Iceberg property names), then the standard
//! `AWS_*` / `AZURE_*` environment variables.

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use iceberg::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig, StorageFactory};
use iceberg::{Error, ErrorKind, Result};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObjectStoreStorageFactory;

#[typetag::serde]
impl StorageFactory for ObjectStoreStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(ObjectStoreStorage { props: config.props().clone(), stores: Default::default() }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStoreStorage {
    props: HashMap<String, String>,
    #[serde(skip)]
    stores: Arc<Mutex<HashMap<String, Arc<dyn ObjectStore>>>>,
}

fn err(kind: ErrorKind, msg: impl Into<String>) -> Error {
    Error::new(kind, msg)
}

fn os_err(e: object_store::Error, path: &str) -> Error {
    let kind = match e {
        object_store::Error::NotFound { .. } => ErrorKind::DataInvalid,
        _ => ErrorKind::Unexpected,
    };
    Error::new(kind, format!("object store error for {path}")).with_source(e)
}

/// Map Iceberg storage property names onto object_store configuration keys.
pub fn object_store_options(url: &url::Url, props: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut opts: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("AWS_") || k.starts_with("AZURE_"))
        .map(|(k, v)| (k.to_ascii_lowercase(), v))
        .collect();
    let mut set = |k: &str, v: &str| {
        opts.retain(|(ek, _)| ek != k);
        opts.push((k.to_string(), v.to_string()));
    };
    for (k, v) in props {
        match k.as_str() {
            "s3.access-key-id" => set("aws_access_key_id", v),
            "s3.secret-access-key" => set("aws_secret_access_key", v),
            "s3.session-token" => set("aws_session_token", v),
            "s3.region" | "client.region" => set("aws_region", v),
            "s3.endpoint" => set("aws_endpoint", v),
            "s3.path-style-access" => set("aws_virtual_hosted_style_request", if v == "true" { "false" } else { "true" }),
            "adls.account-name" => set("azure_storage_account_name", v),
            "adls.account-key" | "adls.auth.shared-key.account.key" => set("azure_storage_account_key", v),
            "adls.auth.shared-key.account.name" => set("azure_storage_account_name", v),
            "adls.connection-string" => set("azure_storage_connection_string", v),
            _ => {}
        }
        // Vended SAS tokens are keyed by account host: "adls.sas-token.<account>.dfs.core.windows.net"
        if let Some(account_host) = k.strip_prefix("adls.sas-token.") {
            let host = url.host_str().unwrap_or_default();
            let account_in_url = host.split('.').next().unwrap_or_default();
            let account_in_key = account_host.split('.').next().unwrap_or_default();
            let user = url.username();
            if account_in_url == account_in_key || host == account_host || (!user.is_empty() && account_host.starts_with(account_in_url)) {
                set("azure_storage_sas_key", v);
            }
        }
    }
    if let Some(ep) = props.get("s3.endpoint") {
        if ep.starts_with("http://") {
            set("aws_allow_http", "true");
        }
    }
    opts
}

impl ObjectStoreStorage {
    fn resolve(&self, path: &str) -> Result<(Arc<dyn ObjectStore>, ObjPath)> {
        let url = url::Url::parse(path).or_else(|_| {
            // bare local path
            url::Url::from_file_path(path).map_err(|_| err(ErrorKind::DataInvalid, format!("invalid path {path}")))
        })?;
        let key = format!("{}://{}@{}", url.scheme(), url.username(), url.host_str().unwrap_or_default());
        let obj_path = if url.scheme() == "file" {
            ObjPath::from_url_path(url.path()).map_err(|e| err(ErrorKind::DataInvalid, e.to_string()))?
        } else {
            ObjPath::from_url_path(url.path().trim_start_matches('/'))
                .map_err(|e| err(ErrorKind::DataInvalid, e.to_string()))?
        };
        let mut stores = self.stores.lock().unwrap();
        if let Some(s) = stores.get(&key) {
            return Ok((s.clone(), obj_path));
        }
        let store: Arc<dyn ObjectStore> = if url.scheme() == "file" {
            Arc::new(object_store::local::LocalFileSystem::new())
        } else {
            let opts = object_store_options(&url, &self.props);
            let (s, _) = object_store::parse_url_opts(&url, opts)
                .map_err(|e| Error::new(ErrorKind::FeatureUnsupported, format!("no object store for {path}")).with_source(e))?;
            Arc::from(s)
        };
        stores.insert(key, store.clone());
        Ok((store, obj_path))
    }
}

struct Reader {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    display: String,
}

#[async_trait::async_trait]
impl FileRead for Reader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store.get_range(&self.path, range).await.map_err(|e| os_err(e, &self.display))
    }
}

struct Writer {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    display: String,
    buf: Vec<Bytes>,
    closed: bool,
}

#[async_trait::async_trait]
impl FileWrite for Writer {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        if self.closed {
            return Err(err(ErrorKind::Unexpected, "write after close"));
        }
        self.buf.push(bs);
        Ok(())
    }
    async fn close(&mut self) -> Result<FileMetadata> {
        if self.closed {
            return Err(err(ErrorKind::Unexpected, "file already closed"));
        }
        self.closed = true;
        let size: usize = self.buf.iter().map(|b| b.len()).sum();
        let payload = PutPayload::from_iter(std::mem::take(&mut self.buf));
        self.store.put(&self.path, payload).await.map_err(|e| os_err(e, &self.display))?;
        Ok(FileMetadata { size: size as u64 })
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Storage for ObjectStoreStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (s, p) = self.resolve(path)?;
        match s.head(&p).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(os_err(e, path)),
        }
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (s, p) = self.resolve(path)?;
        let m = s.head(&p).await.map_err(|e| os_err(e, path))?;
        Ok(FileMetadata { size: m.size })
    }
    async fn read(&self, path: &str) -> Result<Bytes> {
        let (s, p) = self.resolve(path)?;
        s.get(&p).await.map_err(|e| os_err(e, path))?.bytes().await.map_err(|e| os_err(e, path))
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (store, p) = self.resolve(path)?;
        Ok(Box::new(Reader { store, path: p, display: path.to_string() }))
    }
    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (s, p) = self.resolve(path)?;
        s.put(&p, PutPayload::from_bytes(bs)).await.map_err(|e| os_err(e, path))?;
        Ok(())
    }
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (store, p) = self.resolve(path)?;
        Ok(Box::new(Writer { store, path: p, display: path.to_string(), buf: vec![], closed: false }))
    }
    async fn delete(&self, path: &str) -> Result<()> {
        let (s, p) = self.resolve(path)?;
        match s.delete(&p).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(os_err(e, path)),
        }
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (s, p) = self.resolve(path)?;
        let mut list = s.list(Some(&p));
        while let Some(m) = list.next().await {
            let m = m.map_err(|e| os_err(e, path))?;
            s.delete(&m.location).await.map_err(|e| os_err(e, path))?;
        }
        Ok(())
    }
    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        while let Some(p) = paths.next().await {
            self.delete(&p).await?;
        }
        Ok(())
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vended_azure_sas_is_matched_to_account() {
        let url = url::Url::parse("abfss://lake@acct1.dfs.core.windows.net/wh/t/data.parquet").unwrap();
        let props = HashMap::from([
            ("adls.sas-token.acct1.dfs.core.windows.net".to_string(), "sv=1&sig=abc".to_string()),
            ("adls.sas-token.other.dfs.core.windows.net".to_string(), "nope".to_string()),
        ]);
        let opts = object_store_options(&url, &props);
        assert!(opts.contains(&("azure_storage_sas_key".to_string(), "sv=1&sig=abc".to_string())));
        assert!(!opts.iter().any(|(_, v)| v == "nope"));
    }

    #[test]
    fn s3_props_map() {
        let url = url::Url::parse("s3://bucket/x").unwrap();
        let props = HashMap::from([
            ("s3.access-key-id".to_string(), "AK".to_string()),
            ("s3.endpoint".to_string(), "http://minio:9000".to_string()),
            ("s3.path-style-access".to_string(), "true".to_string()),
        ]);
        let opts = object_store_options(&url, &props);
        assert!(opts.contains(&("aws_access_key_id".into(), "AK".into())));
        assert!(opts.contains(&("aws_allow_http".into(), "true".into())));
        assert!(opts.contains(&("aws_virtual_hosted_style_request".into(), "false".into())));
    }
}
