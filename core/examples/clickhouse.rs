#![cfg(feature = "clickhouse")]

use datafusion::catalog::TableProviderFactory;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use datafusion_table_providers::clickhouse::ClickHouseTableProviderFactory;
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

    // Create a DataFusion SessionContext
    let ctx = SessionContext::new();

    // Define the connection string (replace with your actual ClickHouse connection details)
    // Format: tcp://[user[:password]@]host[:port][/database][?options]
    // Example using default user, no password, localhost:9000, default database:
    let connection_string = "tcp://localhost:9000/default";
    // Example with user/password:
    // let connection_string = "tcp://myuser:mypassword@localhost:9000/default";

    println!("Connecting to ClickHouse at: {}", connection_string);

    // Create ClickHouse table provider factory
    let factory = ClickHouseTableProviderFactory::new();

    // Create a mock CreateExternalTable command for the factory
    use datafusion::common::{Constraints, DFSchema};
    use datafusion::logical_expr::CreateExternalTable;
    use std::collections::HashMap;

    let mut options = HashMap::new();
    options.insert(
        "connection_string".to_string(),
        connection_string.to_string(),
    );

    // Create a simple schema for demonstration
    let schema = DFSchema::empty();

    let create_table_cmd = CreateExternalTable {
        name: TableReference::bare("my_table"),
        location: "default.my_table".to_string(),
        file_type: "clickhouse".to_string(),
        schema: Arc::new(schema),
        table_partition_cols: vec![],
        if_not_exists: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options,
        constraints: Constraints::empty(),
        column_defaults: HashMap::new(),
        temporary: false,
    };

    // Create the table provider using the factory
    let table_provider = factory.create(&ctx.state(), &create_table_cmd).await?;

    // Register the table provider with the context
    ctx.register_table("clickhouse_example", table_provider)?;

    println!("External table 'clickhouse_example' registered.");

    // Query the external table
    let df = ctx.sql("SELECT * FROM clickhouse_example LIMIT 10").await?;

    println!("Querying external table...");

    // Print the results
    df.show().await?;

    println!("ClickHouse example finished successfully.");

    Ok(())
}
