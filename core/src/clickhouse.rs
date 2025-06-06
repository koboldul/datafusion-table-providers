#![cfg(feature = "clickhouse")]

mod sql_table;
pub use self::sql_table::ClickHouseTable;

use crate::sql::db_connection_pool::clickhousepool::ClickHouseConnectionPool;
use crate::sql::db_connection_pool::{
    clickhousepool::ClickHouseConnectionPoolFactory, DbConnectionPool,
};
use crate::sql::sql_provider_datafusion::SqlTable;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

// Re-export pool and factory for easier use
// REMOVE THIS DUPLICATE IMPORT BLOCK

// TODO: Define ClickHouseTable struct (likely in a submodule like core/src/clickhouse/sql_table.rs)
// ClickHouseTable is now re-exported from sql_table.rs

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing 'connection_string' option for ClickHouse table"))]
    MissingConnectionString {},

    #[snafu(display("Unable to create ClickHouse connection pool: {source}"))]
    UnableToCreatePool {
        source: crate::sql::db_connection_pool::clickhousepool::Error,
    },

    #[snafu(display("Unable to infer schema for table '{table_name}': {source}"))]
    UnableToInferSchema {
        table_name: String,
        source: crate::sql::db_connection_pool::dbconnection::Error,
    },

    #[snafu(display("Table '{table_name}' not found in ClickHouse"))]
    TableNotFound { table_name: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub struct ClickHouseTableFactory {
    pool: Arc<ClickHouseConnectionPool>,
}

impl ClickHouseTableFactory {
    #[must_use]
    pub fn new(pool: Arc<ClickHouseConnectionPool>) -> Self {
        Self { pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);

        let table_provider = Arc::new(
            ClickHouseTable::new(&pool, table_reference)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        // Note: Federation support would need to be implemented for ClickHouseTable
        // #[cfg(feature = "clickhouse-federation")]
        // let table_provider = Arc::new(
        //     table_provider
        //         .create_federated_table_provider()
        //         .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        // );

        Ok(table_provider)
    }

    // pub async fn read_write_table_provider(
    //     &self,
    //     table_reference: TableReference,
    // ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
    //     let read_provider = Self::table_provider(self, table_reference.clone()).await?;
    //     let schema = read_provider.schema();

    //     let postgres = Postgres::new(
    //         table_reference,
    //         Arc::clone(&self.pool),
    //         schema,
    //         Constraints::empty(),
    //     );

    //     Ok(PostgresTableWriter::create(read_provider, postgres, None))
    // }
}

#[derive(Default, Debug)]
pub struct ClickHouseTableProviderFactory {}

impl ClickHouseTableProviderFactory {
    pub fn new() -> Self {
        Self {}
    }

    fn get_connection_string(options: &HashMap<String, String>) -> Result<String> {
        options
            .get("connection_string")
            .cloned()
            .context(MissingConnectionStringSnafu)
    }
}

#[async_trait]
impl datafusion::catalog::TableProviderFactory for ClickHouseTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let connection_string =
            Self::get_connection_string(&cmd.options).map_err(|e| to_datafusion_error(e))?;

        let pool_factory = ClickHouseConnectionPoolFactory::new(&connection_string, "default")
            .context(UnableToCreatePoolSnafu)
            .map_err(|e| to_datafusion_error(e))?;

        let pool = Arc::new(
            pool_factory
                .build()
                .await
                .map_err(DataFusionError::External)?,
        );

        let table_ref = TableReference::from(cmd.name.clone());

        // Use SqlTable instead of custom ClickHouseTable implementation
        let dyn_pool: Arc<dyn DbConnectionPool<clickhouse::Client, String> + Send + Sync> = pool;

        let table_provider = Arc::new(
            SqlTable::new("clickhouse", &dyn_pool, table_ref)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );

        Ok(table_provider)
    }
}
