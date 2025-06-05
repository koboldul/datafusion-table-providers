#![cfg(feature = "clickhouse")]

use clickhouse::Client;

use crate::sql::db_connection_pool::{
    dbconnection::{clickhouseconn::ClickHouseConnection, DbConnection},
    DbConnectionPool, JoinPushDown, Result,
};
use async_trait::async_trait;
use clickhouse::error::Error as ClickHouseError;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create ClickHouse connection pool: {source}"))]
    UnableToCreatePool { source: ClickHouseError },

    #[snafu(display("Invalid ClickHouse connection string: {source}"))]
    InvalidConnectionString { source: url::ParseError },

    #[snafu(display("Missing database name in ClickHouse connection string"))]
    MissingDatabaseName {},
}

pub struct ClickHouseConnectionPoolFactory {
    dsn: String,
    // Add other pool configuration options here if needed (e.g., min_idle, max_size)
}

impl ClickHouseConnectionPoolFactory {
    /// Creates a new factory from a ClickHouse connection string URL.
    /// Example: tcp://user:password@host:port/database?options...
    pub fn new(connection_string: &str) -> Result<Self, Error> {
        Ok(Self {
            dsn: connection_string.to_string(),
        })
    }

    /// Builds the `ClickHouseConnectionPool`.
    pub async fn build(&self) -> Result<ClickHouseConnectionPool> {
        let client = Client::default().with_url(&self.dsn);
        // Test connection? client.ping().await might do this implicitly or error later.
        // Let's assume the client creation itself is enough for now.

        // Determine JoinPushDown based on the database name
        // Extract database name from DSN string (simple parse)
        let db_name = self
            .dsn
            .split('/')
            .last()
            .and_then(|s| s.split('?').next())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::MissingDatabaseName {})?;
        let join_push_down = JoinPushDown::AllowedFor(db_name.to_string());

        Ok(ClickHouseConnectionPool {
            client,
            join_push_down,
        })
    }
}

pub struct ClickHouseConnectionPool {
    pub client: Client,
    join_push_down: JoinPushDown,
}

#[async_trait]
impl DbConnectionPool<Client, String> for ClickHouseConnectionPool {
    async fn connect(&self) -> Result<Box<dyn DbConnection<Client, String>>> {
        Ok(Box::new(ClickHouseConnection::new(self.client.clone())))
    }

    fn join_push_down(&self) -> JoinPushDown {
        self.join_push_down.clone()
    }
}

impl ClickHouseConnectionPool {
    pub async fn connect_direct(&self) -> Result<ClickHouseConnection, Error> {
        // Just clone the client for each connection
        Ok(ClickHouseConnection::new(self.client.clone()))
    }
}
