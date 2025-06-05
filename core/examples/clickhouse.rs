#![cfg(feature = "clickhouse")]

use datafusion::catalog::TableProviderFactory;
use datafusion::prelude::*;
use datafusion::sql::{
    db_connection_pool::clickhousepool::ClickHouseConnectionPoolFactory, TableReference,
};
use datafusion_table_providers::clickhouse::ClickHouseTableProviderFactory;
use datafusion_table_providers::common::DatabaseCatalogProvider;
use std::sync::Arc;

/// This example demonstrates querying a ClickHouse database.
///
/// To run this example, ensure you have a ClickHouse instance running
/// and provide the correct connection string.
///
/// For example, using Docker:
/// docker run -d -p 8123:8123 -p 9000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
///
/// Then, you might connect using a tool like `clickhouse-client` and create a table:
/// CREATE TABLE default.my_table (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;
/// INSERT INTO default.my_table VALUES (1, 'one'), (2, 'two');
///
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running ClickHouse example...");
    println!(
        "Make sure you have a ClickHouse instance running and a table named 'default.my_table'."
    );

    let connection_string = "http://ice@localhost:8123/default";
    println!("Connecting to ClickHouse at: {}", connection_string);

    let ch_pool = Arc::new(
        ClickHouseConnectionPoolFactory::new(connection_string)
            .await
            .expect("unable to create Clickhouse connection pool"),
    );

    let catalog = DatabaseCatalogProvider::try_new(ch_pool).await.unwrap();

    // Used to generate TableProvider instances that can read PostgreSQL table data
    // let table_factory = ClickhouseTableFactory::new(ch_pool.clone());

    // Create a DataFusion SessionContext
    let ctx = SessionContext::new();

    // ctx.register_catalog("default", table_provider)?;

    // Query the external table
    let df = ctx.sql("SELECT * FROM datafusion_test LIMIT 10").await?;

    println!("Querying external table...");

    // Print the results
    df.show().await?;

    println!("ClickHouse example finished successfully.");

    Ok(())
}
