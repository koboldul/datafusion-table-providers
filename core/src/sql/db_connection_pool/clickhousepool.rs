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
    // Add other pool configuration options here if needed (e.g., min_idle, max_size),
    catalog: String,
}

impl ClickHouseConnectionPoolFactory {
    /// Creates a new factory from a ClickHouse connection string URL.
    /// Example: tcp://user:password@host:port/database?options...
    pub fn new(
        connection_string: impl Into<String>,
        catalog: impl Into<String>,
    ) -> Result<Self, Error> {
        Ok(Self {
            dsn: connection_string.into(),
            catalog: catalog.into(),
        })
    }

    /// Builds the `ClickHouseConnectionPool`.
    pub async fn build(&self) -> Result<ClickHouseConnectionPool> {
        // Create client with base URL (without database path)
        let client = Client::default()
            .with_url(&self.dsn)
            .with_user("ice")
            .with_password("")
            .with_database(&self.catalog) // Set default database
            .with_compression(clickhouse::Compression::None); // Forces POST requests by using compression setting

        // Use "default" as the database name for JoinPushDown
        let join_push_down = JoinPushDown::AllowedFor("default".to_string());

        Ok(ClickHouseConnectionPool {
            client,
            join_push_down,
        })
    }
}

/*****************
 * Connection Pool
 ******************/

pub type DynClickHouseConnectionPool = dyn DbConnectionPool<Client, String> + Send + Sync;

pub struct ClickHouseConnectionPool {
    pub client: Client,
    join_push_down: JoinPushDown,
}

impl ClickHouseConnectionPool {
    pub async fn connect_direct(&self) -> Result<ClickHouseConnection, Error> {
        // Just clone the client for each connection
        Ok(ClickHouseConnection::new(self.client.clone()))
    }
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
