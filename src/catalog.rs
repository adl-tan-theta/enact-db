//! Federated SQL catalog: one DataFusion `SessionContext` that sees every
//! configured enterprise source plus the gateway's own live system tables
//! (agent tool calls, agent KV, CRDT documents).

use crate::config::{AzureConfig, SourceConfig};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use futures::future::BoxFuture;
use std::sync::Arc;

pub async fn build_context(sources: &[SourceConfig]) -> Result<SessionContext> {
    let config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("gateway", "public");
    let ctx = SessionContext::new_with_config(config);
    for src in sources {
        register_source(&ctx, src)
            .await
            .with_context(|| format!("registering source '{}'", src.name()))?;
        tracing::info!(source = src.name(), "registered source");
    }
    Ok(ctx)
}

async fn register_source(ctx: &SessionContext, src: &SourceConfig) -> Result<()> {
    match src {
        SourceConfig::Parquet { name, location, azure } => {
            ensure_object_store(ctx, location, azure.as_ref())?;
            ctx.register_parquet(name, location, ParquetReadOptions::default()).await?;
        }
        SourceConfig::Csv { name, location, azure } => {
            ensure_object_store(ctx, location, azure.as_ref())?;
            ctx.register_csv(name, location, CsvReadOptions::new().has_header(true)).await?;
        }
        SourceConfig::Json { name, location, azure } => {
            ensure_object_store(ctx, location, azure.as_ref())?;
            ctx.register_json(name, location, JsonReadOptions::default()).await?;
        }
        SourceConfig::Postgres { name, connection, table } => {
            register_postgres(ctx, name, connection, table).await?;
        }
        SourceConfig::Snowflake { name, driver, uri, table } => {
            register_snowflake(ctx, name, driver, uri, table).await?;
        }
    }
    Ok(())
}

/// Register an Azure object store for `az://`/`abfss://` locations. Local paths need nothing.
fn ensure_object_store(ctx: &SessionContext, location: &str, azure: Option<&AzureConfig>) -> Result<()> {
    let is_azure = location.starts_with("az://")
        || location.starts_with("azure://")
        || location.starts_with("abfs://")
        || location.starts_with("abfss://");
    if !is_azure {
        return Ok(());
    }
    #[cfg(feature = "azure")]
    {
        use object_store::azure::MicrosoftAzureBuilder;
        let url = url::Url::parse(location).context("invalid object store URL")?;
        let cfg = azure.cloned().unwrap_or_default();
        let mut builder = MicrosoftAzureBuilder::from_env().with_url(location);
        if let Some(account) = &cfg.account {
            builder = builder.with_account(account);
        }
        if let Some(container) = &cfg.container {
            builder = builder.with_container_name(container);
        }
        if let Some(var) = &cfg.access_key_env {
            let key = std::env::var(var).with_context(|| format!("env var {var} (azure access key) not set"))?;
            builder = builder.with_access_key(key);
        }
        if cfg.use_emulator {
            builder = builder.with_use_emulator(true);
        }
        let store = builder.build()?;
        let base = url::Url::parse(&format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default()))?;
        ctx.register_object_store(&base, Arc::new(store));
        Ok(())
    }
    #[cfg(not(feature = "azure"))]
    {
        let _ = (ctx, azure);
        Err(anyhow!("{location}: binary built without the `azure` feature"))
    }
}

#[cfg(feature = "postgres")]
async fn register_postgres(ctx: &SessionContext, name: &str, connection: &str, table: &str) -> Result<()> {
    use datafusion::common::TableReference;
    use datafusion_table_providers::{
        postgres::PostgresTableFactory, sql::db_connection_pool::postgrespool::PostgresConnectionPool,
        util::secrets::to_secret_map,
    };
    use std::collections::HashMap;
    if connection.starts_with("postgres://") || connection.starts_with("postgresql://") {
        return Err(anyhow!(
            "postgres sources need a key=value connection string (host=... user=... dbname=... sslmode=...)"
        ));
    }
    // The provider defaults to verify-full when sslmode is absent; apply libpq's
    // default (prefer) instead so all Postgres connections behave the same.
    let connection = if connection.contains("sslmode=") {
        connection.to_string()
    } else {
        format!("{connection} sslmode=prefer")
    };
    let params = to_secret_map(HashMap::from([("connection_string".to_string(), connection)]));
    let pool = Arc::new(
        PostgresConnectionPool::new(params)
            .await
            .map_err(|e| anyhow!("postgres pool: {e}"))?,
    );
    let factory = PostgresTableFactory::new(pool);
    let provider = factory
        .table_provider(TableReference::from(table))
        .await
        .map_err(|e| anyhow!("postgres table {table}: {e}"))?;
    ctx.register_table(name, provider)?;
    Ok(())
}

#[cfg(not(feature = "postgres"))]
async fn register_postgres(_: &SessionContext, _: &str, _: &str, _: &str) -> Result<()> {
    Err(anyhow!("binary built without the `postgres` feature"))
}

#[cfg(feature = "snowflake")]
async fn register_snowflake(ctx: &SessionContext, name: &str, driver: &str, uri: &str, table: &str) -> Result<()> {
    use adbc_core::options::{AdbcVersion, OptionDatabase};
    use adbc_core::{Driver, LOAD_FLAG_DEFAULT};
    use adbc_driver_manager::ManagedDriver;
    use datafusion::common::TableReference;
    use datafusion_table_providers::{adbc::AdbcTableFactory, sql::db_connection_pool::adbcpool::ADBCPool};
    let mut drv = ManagedDriver::load_from_name(driver, None, AdbcVersion::V110, LOAD_FLAG_DEFAULT, None)
        .map_err(|e| anyhow!("loading ADBC driver {driver}: {e}"))?;
    let db = drv
        .new_database_with_opts([(OptionDatabase::Uri, uri.into())])
        .map_err(|e| anyhow!("snowflake connect: {e}"))?;
    let pool = Arc::new(ADBCPool::new(db, None).map_err(|e| anyhow!("adbc pool: {e}"))?);
    let factory = AdbcTableFactory::new(pool);
    let provider = factory
        .table_provider(TableReference::from(table), None)
        .await
        .map_err(|e| anyhow!("snowflake table {table}: {e}"))?;
    ctx.register_table(name, provider)?;
    Ok(())
}

#[cfg(not(feature = "snowflake"))]
async fn register_snowflake(_: &SessionContext, _: &str, _: &str, _: &str, _: &str) -> Result<()> {
    Err(anyhow!("binary built without the `snowflake` feature (cargo build --features snowflake)"))
}

// ---------------------------------------------------------------------------
// Live system tables
// ---------------------------------------------------------------------------

pub type Loader = Arc<dyn Fn() -> BoxFuture<'static, Result<RecordBatch>> + Send + Sync>;

/// A table whose rows are produced on every scan by an async loader. Used to
/// expose gateway state (agent audit logs, KV, CRDT docs) to SQL so it can be
/// joined with enterprise data.
pub struct LiveTable {
    schema: SchemaRef,
    loader: Loader,
}

impl LiveTable {
    pub fn new(schema: SchemaRef, loader: Loader) -> Self {
        Self { schema, loader }
    }
}

impl std::fmt::Debug for LiveTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveTable").field("schema", &self.schema).finish()
    }
}

#[async_trait]
impl TableProvider for LiveTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let batch = (self.loader)()
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;
        let mem = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}
