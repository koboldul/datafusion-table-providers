use crate::sql::db_connection_pool::dbconnection::UnsupportedDataTypeSnafu;
use std::{any::Any, sync::Arc};

use crate::sql::db_connection_pool::dbconnection::{
    AsyncDbConnection, DbConnection, Error as DbConnectionError, GenericError, Result,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use clickhouse::{error::Error as ClickHouseError, Client, Row};
use datafusion::{execution::SendableRecordBatchStream, sql::TableReference};
use serde::Deserialize;

pub struct ClickHouseConnection {
    client: Client,
}

impl ClickHouseConnection {
    /// Creates a new `ClickHouseConnection`.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    fn get_client(&self) -> &Client {
        &self.client
    }

    // Helper function to map ClickHouse types to Arrow types
    fn map_clickhouse_type_to_arrow(
        ch_type: &str,
        field_name: &str,
    ) -> Result<arrow::datatypes::DataType, DbConnectionError> {
        // TODO: Implement comprehensive mapping based on
        // https://clickhouse.com/docs/en/interfaces/formats#arrow
        // and https://clickhouse.com/docs/en/sql-reference/data-types/
        // Need to handle: LowCardinality, Nullable, Nested, Enum, UUID, IPv4/6, Decimal, DateTime64 etc.
        match ch_type {
            "Int8" => Ok(arrow::datatypes::DataType::Int8),
            "Int16" => Ok(arrow::datatypes::DataType::Int16),
            "Int32" => Ok(arrow::datatypes::DataType::Int32),
            "Int64" => Ok(arrow::datatypes::DataType::Int64),
            "UInt8" => Ok(arrow::datatypes::DataType::UInt8),
            "UInt16" => Ok(arrow::datatypes::DataType::UInt16),
            "UInt32" => Ok(arrow::datatypes::DataType::UInt32),
            "UInt64" => Ok(arrow::datatypes::DataType::UInt64),
            "Float32" => Ok(arrow::datatypes::DataType::Float32),
            "Float64" => Ok(arrow::datatypes::DataType::Float64),
            "String" | "FixedString" => Ok(arrow::datatypes::DataType::Utf8), // FixedString might need different handling?
            "Date" => Ok(arrow::datatypes::DataType::Date32),
            "DateTime" | "DateTime64" => Ok(arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond, // Assuming microsecond precision for DateTime64, might need adjustment
                None, // No timezone info from ClickHouse type name
            )),
            // Add more mappings here...
            // Example for Nullable:
            // "Nullable(Int32)" => Ok(arrow::datatypes::DataType::Int32), // Nullability handled by Arrow Field
            // Example for LowCardinality:
            // "LowCardinality(String)" => Ok(arrow::datatypes::DataType::Utf8), // Or Dictionary? Needs investigation
            _ => {
                if ch_type.starts_with("Nullable(") && ch_type.ends_with(')') {
                    let inner_type = &ch_type[9..ch_type.len() - 1];
                    Self::map_clickhouse_type_to_arrow(inner_type, field_name)
                } else if ch_type.starts_with("LowCardinality(") && ch_type.ends_with(')') {
                    let inner_type = &ch_type[15..ch_type.len() - 1];
                    // Potentially map to Dictionary, but needs clickhouse-rs support investigation.
                    // For now, map to inner type.
                    Self::map_clickhouse_type_to_arrow(inner_type, field_name)
                }
                // TODO: Handle Nested, Enum, UUID, IPv4/6, Decimal etc.
                else {
                    UnsupportedDataTypeSnafu {
                        data_type: ch_type.to_string(),
                        field_name: field_name.to_string(),
                    }
                    .fail()?
                }
            }
        }
    }
}

impl DbConnection<Client, String> for ClickHouseConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Client, String>> {
        Some(self)
    }
}

#[async_trait]
impl AsyncDbConnection<Client, String> for ClickHouseConnection {
    fn new(client: Client) -> Self {
        Self { client }
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, DbConnectionError> {
        let sql = format!(
            "SELECT name FROM system.tables WHERE database = '{}'",
            schema
        );

        let mut cursor = self.client.query(&sql).fetch::<String>().map_err(|e| {
            DbConnectionError::UnableToGetTables {
                source: Box::new(e),
            }
        })?;

        let mut tables = Vec::new();
        while let Some(row) =
            cursor
                .next()
                .await
                .map_err(|e| DbConnectionError::UnableToGetTables {
                    source: Box::new(e),
                })?
        {
            tables.push(row);
        }
        Ok(tables)
    }

    async fn schemas(&self) -> Result<Vec<String>, DbConnectionError> {
        let sql = "SELECT name FROM system.databases";

        let mut cursor = self.client.query(sql).fetch::<String>().map_err(|e| {
            DbConnectionError::UnableToGetSchemas {
                source: Box::new(e),
            }
        })?;

        let mut schemas = Vec::new();
        while let Some(row) =
            cursor
                .next()
                .await
                .map_err(|e| DbConnectionError::UnableToGetSchemas {
                    source: Box::new(e),
                })?
        {
            schemas.push(row);
        }
        Ok(schemas)
    }

    async fn get_schema(
        &self,
        table_reference: &TableReference,
    ) -> Result<SchemaRef, DbConnectionError> {
        let database = table_reference.schema().unwrap_or("default");
        let table = table_reference.table();

        let sql = format!(
            "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
            database, table
        );

        #[derive(Row, Debug, Deserialize)]
        struct ColumnInfo {
            name: String,
            #[serde(rename = "type")]
            column_type: String,
        }

        let rows = self
            .client
            .query(&sql)
            .fetch_all::<ColumnInfo>()
            .await
            .map_err(|e| DbConnectionError::UndefinedTable {
                table_name: table_reference.to_string(),
                source: Box::new(e),
            })?;

        if rows.is_empty() {
            return Err(DbConnectionError::UndefinedTable {
                table_name: table_reference.to_string(),
                source: Box::new(ClickHouseError::Custom("Table not found".to_string())),
            });
        }

        let mut fields = Vec::new();
        for row in rows {
            let arrow_type = Self::map_clickhouse_type_to_arrow(&row.column_type, &row.name)?;
            let is_nullable = row.column_type.starts_with("Nullable(");
            fields.push(arrow::datatypes::Field::new(
                row.name,
                arrow_type,
                is_nullable,
            ));
        }

        Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
    }

    async fn query_arrow(
        &self,
        sql: &str,
        _params: &[String],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        // For now, implement a basic query that returns the projected schema
        // This is a placeholder implementation that should be improved

        let schema = projected_schema.unwrap_or_else(|| {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("result", arrow::datatypes::DataType::Utf8, true),
            ]))
        });

        // For now, create an empty stream with the correct schema
        // This is a placeholder - real implementation would execute the ClickHouse query
        let batches: Vec<arrow::record_batch::RecordBatch> =
            vec![arrow::record_batch::RecordBatch::new_empty(schema.clone())];

        use datafusion::physical_plan::memory::MemoryStream;
        let stream = MemoryStream::try_new(batches, schema, None)
            .map_err(|e| Box::new(e) as GenericError)?;

        Ok(Box::pin(stream))
    }

    async fn execute(&self, sql: &str, _params: &[String]) -> Result<u64> {
        self.client
            .query(sql)
            .execute()
            .await
            .map(|_| 0u64) // ClickHouse execute doesn't return affected rows count, return 0
            .map_err(|e| Box::new(e) as GenericError)
    }
}
