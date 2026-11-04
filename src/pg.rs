//! Small Postgres client helper (cloud memory + transcript sink).
#![cfg(feature = "postgres")]

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A reconnecting Postgres client. TLS is negotiated according to `sslmode`
/// in the connection string (Azure Database for PostgreSQL requires it).
pub struct Pg {
    conn_str: String,
    client: Mutex<Option<Arc<tokio_postgres::Client>>>,
}

impl Pg {
    pub fn new(conn_str: &str) -> Self {
        Self { conn_str: conn_str.to_string(), client: Mutex::new(None) }
    }

    pub async fn client(&self) -> Result<Arc<tokio_postgres::Client>> {
        let mut guard = self.client.lock().await;
        if let Some(c) = guard.as_ref() {
            if !c.is_closed() {
                return Ok(c.clone());
            }
        }
        let (conn_str, verify) = libpq_sslmode(&self.conn_str);
        let mut b = native_tls::TlsConnector::builder();
        match verify {
            // libpq semantics: prefer/require encrypt but do not verify the certificate
            Verify::None => {
                b.danger_accept_invalid_certs(true).danger_accept_invalid_hostnames(true);
            }
            Verify::Ca => {
                b.danger_accept_invalid_hostnames(true);
            }
            Verify::Full => {}
        }
        let tls = postgres_native_tls::MakeTlsConnector::new(b.build().context("tls connector")?);
        let (client, connection) = tokio_postgres::connect(&conn_str, tls)
            .await
            .context("connecting to postgres")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "postgres connection closed");
            }
        });
        let client = Arc::new(client);
        *guard = Some(client.clone());
        Ok(client)
    }
}

#[derive(Debug, PartialEq)]
enum Verify {
    None,
    Ca,
    Full,
}

/// tokio-postgres only understands sslmode=disable|prefer|require. Map libpq's
/// verify-ca / verify-full onto `require` + the matching certificate checks.
fn libpq_sslmode(conn: &str) -> (String, Verify) {
    for (mode, v) in [("verify-full", Verify::Full), ("verify-ca", Verify::Ca)] {
        let needle = format!("sslmode={mode}");
        if conn.contains(&needle) {
            return (conn.replace(&needle, "sslmode=require"), v);
        }
    }
    (conn.to_string(), Verify::None)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sslmode_mapping() {
        assert_eq!(libpq_sslmode("host=a sslmode=verify-full").0, "host=a sslmode=require");
        assert_eq!(libpq_sslmode("postgres://u@h/db?sslmode=verify-ca").1, Verify::Ca);
        assert_eq!(libpq_sslmode("host=a sslmode=require").1, Verify::None);
    }
}
