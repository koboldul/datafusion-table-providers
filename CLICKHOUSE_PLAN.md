# Plan for ClickHouse Table Provider Implementation

This document outlines the plan for adding a new DataFusion table provider for ClickHouse within the `datafusion-table-providers` project.

## Requirements

*   Implement a table provider for ClickHouse.
*   Use the `clickhouse-rs` crate (<https://github.com/suharev7/clickhouse-rs>) for database communication.
*   Place the provider implementation behind a `clickhouse` feature flag.
*   Include connection and connection pool management.
*   Support simple query execution (schema inference and scanning).
*   Handle ClickHouse-specific types like `LowCardinality` and `Nested` during schema inference.
*   Provide examples of usage.

## Analysis of Existing Structure

*   **Modularity:** Providers reside in `core/src/[database]`.
*   **Feature Flags:** Used in `core/Cargo.toml` for conditional compilation.
*   **Factories:** `[Database]TableProviderFactory` implements `TableProviderFactory` for parsing options and creating providers.
*   **Connection Pooling:** Abstracted via `DbConnectionPool` trait (`core/src/sql/db_connection_pool/mod.rs`), with specific implementations (e.g., `SqliteConnectionPool`).
*   **Connection Abstraction:** `DbConnection`, `AsyncDbConnection`, `SyncDbConnection` traits (`core/src/sql/db_connection_pool/dbconnection.rs`) unify database operations. Specific implementations (e.g., `SqliteConnection`) adapt drivers.
*   **Table Implementation:** `[Database]Table` implements `TableProvider` for DataFusion interaction.
*   **SQL Generation:** Helpers exist in `core/src/sql/arrow_sql_gen/`.
*   **Examples & Tests:** Located in `core/examples/` and `core/tests/`.

## Implementation Steps

1.  **Setup Feature Flag & Dependencies (`core/Cargo.toml`):**
    *   Add `clickhouse-rs = { version = "...", optional = true }`. Determine the appropriate version.
    *   *(Optional)* Add `bb8 = { version = "...", optional = true }` if using `bb8` for pooling.
    *   Define feature `clickhouse = ["dep:clickhouse-rs", /* other needed deps like async-stream, arrow-schema */]`.
    *   *(Optional)* Define `clickhouse-federation = ["clickhouse", "federation"]`.

2.  **Implement Connection (`core/src/sql/db_connection_pool/dbconnection/clickhouseconn.rs`):**
    *   Create `ClickHouseConnection` struct wrapping `clickhouse_rs::Pool` or a single connection.
    *   Implement `AsyncDbConnection<clickhouse_rs::Pool, clickhouse_rs::query::Query>`:
        *   `get_schema`: Query `system.columns`. Map ClickHouse types to Arrow `SchemaRef`.
            *   **Handle `LowCardinality(T)`:** Map to Arrow `Dictionary` if possible, else underlying `T`.
            *   **Handle `Nested(...)`:** Map to Arrow `List(Struct(...))`.
            *   **Handle other types:** Map `Enum`, `UUID`, `IPv4/6`, `Decimal`, `DateTime64`, etc., correctly.
        *   `query_arrow`: Execute SQL via `clickhouse-rs`, return `SendableRecordBatchStream`.
        *   `execute`: Implement DML/DDL execution.
        *   `tables`, `schemas`: Implement listing.
    *   Implement `DbConnection` trait.
    *   Use `#[cfg(feature = "clickhouse")]`.

3.  **Implement Connection Pool (`core/src/sql/db_connection_pool/clickhousepool.rs`):**
    *   Create `ClickHouseConnectionPoolFactory` to parse connection options (host, port, user, password, database, pool settings).
    *   Create `ClickHouseConnectionPool` struct (using `clickhouse_rs::Pool` or `bb8`).
    *   Implement `DbConnectionPool<clickhouse_rs::Pool, clickhouse_rs::query::Query>`:
        *   `connect`: Return `Box<dyn DbConnection<...>>` wrapping `ClickHouseConnection`.
        *   `join_push_down`: Determine based on connection details.
    *   Use `#[cfg(feature = "clickhouse")]`.

4.  **Implement Provider Core (`core/src/clickhouse.rs`):**
    *   Create `ClickHouseTableProviderFactory`:
        *   Implement `TableProviderFactory`.
        *   `create`: Parse options, use `ClickHouseConnectionPoolFactory`, instantiate `ClickHouseTable`.
    *   *(Optional)* Create `ClickHouse` struct for table-specific logic.
    *   Use `#[cfg(feature = "clickhouse")]`.

5.  **Implement Table Provider (`core/src/clickhouse/sql_table.rs` or similar):**
    *   Create `ClickHouseTable` struct (holds pool, schema).
    *   Implement `TableProvider`:
        *   `schema()`: Return schema.
        *   `scan()`: Use pool, get connection, execute DataFusion scan plan. Consider using `datafusion-federation::sql::SqlTable`.
    *   Use `#[cfg(feature = "clickhouse")]`.

6.  **Integrate Modules:**
    *   `core/src/lib.rs`: Add `#[cfg(feature = "clickhouse")] pub mod clickhouse;`.
    *   `core/src/sql/db_connection_pool/mod.rs`: Add `#[cfg(feature = "clickhouse")] pub mod clickhousepool;`.
    *   `core/src/sql/db_connection_pool/dbconnection.rs`: Add `#[cfg(feature = "clickhouse")] pub mod clickhouseconn;`.

7.  **Add Example (`core/examples/clickhouse.rs`):**
    *   Demonstrate `CREATE EXTERNAL TABLE` and basic queries.
    *   Add to `core/Cargo.toml` with `required-features = ["clickhouse"]`.

8.  **Add Tests (`core/tests/clickhouse/`):**
    *   Set up integration tests (potentially using Docker via `bollard`).
    *   Test schema inference (including special types), queries, connection options.

## Diagram

```mermaid
graph TD
    A[User: CREATE EXTERNAL TABLE clickhouse_table OPTIONS(...)] --> B(DataFusion);
    B --> C{ClickHouseTableProviderFactory};
    C -- Parses Options --> D{ClickHouseConnectionPoolFactory};
    D -- Creates/Gets Pool --> E[ClickHouseConnectionPool (using clickhouse-rs Pool / bb8)];
    C -- Instantiates --> F[ClickHouseTable (TableProvider)];
    F -- Holds Ref --> E;

    subgraph Execution
        G[DataFusion Plan Scan] --> F;
        F -- Gets Connection --> E;
        E -- Provides --> H[ClickHouseConnection (DbConnection)];
        H -- Uses --> I[clickhouse-rs Crate];
        I -- Interacts --> J[(ClickHouse DB)];
        H -- Returns Results --> F;
        F -- Returns RecordBatches --> G;
    end

    subgraph Connection Abstraction
        K(DbConnection Trait) <|-- H;
        L(AsyncDbConnection Trait) <|-- H;
        M(DbConnectionPool Trait) <|-- E;
    end

    style J fill:#f9f,stroke:#333,stroke-width:2px