#![cfg(feature = "clickhouse")]
use datafusion::prelude::SessionContext;
use datafusion_table_providers::{
    common::DatabaseCatalogProvider,
    sql::db_connection_pool::clickhousepool::ClickHouseConnectionPoolFactory,
    sql::db_connection_pool::DbConnectionPool,
};
use std::sync::Arc;

/// This example demonstrates creating a ClickHouse connection pool.
///
/// To run this example with a real ClickHouse instance, ensure you have one running
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
async fn main() {
    println!("Running ClickHouse example...");
    println!("This example demonstrates ClickHouse connection pool creation.");

    let connection_string = "http://localhost:8123";
    println!(
        "Creating ClickHouse connection pool for: {}",
        connection_string
    );

    let factory = match ClickHouseConnectionPoolFactory::new(connection_string, "default") {
        Ok(factory) => factory,
        Err(e) => {
            eprintln!("Failed to create ClickHouse connection pool factory: {}", e);
            return;
        }
    };

    let ch_pool = match factory.build().await {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            eprintln!("Failed to build ClickHouse connection pool: {}", e);
            return;
        }
    };

    println!("ClickHouse connection pool created successfully!");

    // Create database catalog provider
    let catalog = DatabaseCatalogProvider::try_new(ch_pool.clone())
        .await
        .or_else(|err| {
            eprintln!("Failed to create ClickHouse catalog provider: {}", err);
            Err(err)
        })
        .expect("Failed to create ClickHouse catalog provider");

    // Create DataFusion session context
    let ctx = SessionContext::new();
    // Register ClickHouse catalog, making it accessible via the "clickhouse" name
    ctx.register_catalog("clickhouse", Arc::new(catalog));

    // Simple query test
    let df_simple = ctx.sql("SELECT 1").await.expect("select 1 failed");
    df_simple.show().await.expect("show failed");

    // Test basic connection creation (without actually connecting to a server)
    match ch_pool.connect().await {
        Ok(_conn) => {
            println!("Connection object created successfully!");
            println!(
                "Note: Actual database operations would require a running ClickHouse instance."
            );
        }
        Err(e) => {
            println!(
                "Connection creation failed (expected without running server): {}",
                e
            );
            println!("This is normal when no ClickHouse server is running.");
        }
    }

    println!("ClickHouse example finished successfully.");
}
