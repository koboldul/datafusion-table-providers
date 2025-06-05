#![cfg(feature = "clickhouse")]

mod sql_table;
pub use self::sql_table::ClickHouseTable;

use crate::sql::db_connection_pool::{
    clickhousepool::{ClickHouseConnectionPool, ClickHouseConnectionPoolFactory},
    DbConnectionPool,
};
use crate::sql::sql_provider_datafusion::SqlTable; // Assuming we might use this helper
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
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

        let pool_factory = ClickHouseConnectionPoolFactory::new(&connection_string)
            .context(UnableToCreatePoolSnafu)
            .map_err(|e| to_datafusion_error(e))?;

        let pool = pool_factory
            .build()
            .await
            .map_err(DataFusionError::External)?;
        let client = Arc::new(pool.client.clone());

        // Infer schema - requires DbConnectionPool::connect and DbConnection::get_schema to be implemented
        let table_ref = TableReference::from(cmd.name.clone());
        // let conn = pool.connect().await.map_err(|e| DataFusionError::External(Box::new(e)))?;
        // let schema = crate::sql::db_connection_pool::dbconnection::get_schema(conn, &table_ref)
        //     .await
        //     .context(UnableToInferSchemaSnafu {
        //         table_name: table_ref.to_string(),
        //     })
        //     .map_err(|e| to_datafusion_error(e))?;
        // For now, assume schema is provided or use a placeholder
        // Convert DFSchema to Arrow Schema
        let arrow_schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());

        // TODO: Validate provided schema against inferred schema if cmd.schema is provided?

        // Create the table provider instance
        let table_provider = Arc::new(ClickHouseTable::new(client, table_ref, arrow_schema));

        Ok(table_provider)
    }
}
