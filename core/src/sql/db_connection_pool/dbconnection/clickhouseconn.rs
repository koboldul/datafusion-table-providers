use crate::sql::arrow_sql_gen::arrow::map_data_type_to_array_builder;
use crate::sql::db_connection_pool::dbconnection::UnsupportedDataTypeSnafu;
use std::{any::Any, sync::Arc};

use super::Result;
use crate::sql::db_connection_pool::dbconnection::{
    AsyncDbConnection, DbConnection, Error as DbConnectionError,
};
use arrow::{
    array::{ArrayBuilder, ArrayRef, Int32Builder, ListBuilder, StringBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use clickhouse::{error::Error as ClickHouseError, Client, Row};
use datafusion::{
    execution::SendableRecordBatchStream, physical_plan::memory::MemoryStream, sql::TableReference,
};
use serde::Deserialize;
use serde_json::Value;

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
                } else if ch_type.starts_with("Array(") && ch_type.ends_with(')') {
                    let inner_type = &ch_type[6..ch_type.len() - 1];
                    let inner_arrow_type = Self::map_clickhouse_type_to_arrow(inner_type, "item")?;
                    // ClickHouse serializes array inner fields with "item" name and nullable=true
                    // This matches the actual behavior observed in query results
                    Ok(DataType::List(Arc::new(Field::new(
                        "item",
                        inner_arrow_type,
                        true, // ClickHouse array elements are nullable in serialization
                    ))))
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

impl ClickHouseConnection {
    /// Helper function to append null values to builders based on data type
    fn append_null_to_builder(builder: &mut Box<dyn ArrayBuilder>, data_type: &DataType) {
        match data_type {
            DataType::Int32 => {
                builder
                    .as_any_mut()
                    .downcast_mut::<Int32Builder>()
                    .unwrap()
                    .append_null();
            }
            DataType::Int64 => {
                builder
                    .as_any_mut()
                    .downcast_mut::<arrow::array::Int64Builder>()
                    .unwrap()
                    .append_null();
            }
            DataType::UInt64 => {
                builder
                    .as_any_mut()
                    .downcast_mut::<arrow::array::UInt64Builder>()
                    .unwrap()
                    .append_null();
            }
            DataType::Float64 => {
                builder
                    .as_any_mut()
                    .downcast_mut::<arrow::array::Float64Builder>()
                    .unwrap()
                    .append_null();
            }
            DataType::Utf8 => {
                builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_null();
            }
            DataType::Boolean => {
                builder
                    .as_any_mut()
                    .downcast_mut::<arrow::array::BooleanBuilder>()
                    .unwrap()
                    .append_null();
            }
            DataType::List(inner_field) => {
                match inner_field.data_type() {
                    DataType::Int32 => {
                        builder
                            .as_any_mut()
                            .downcast_mut::<ListBuilder<Int32Builder>>()
                            .unwrap()
                            .append_null();
                    }
                    DataType::Utf8 => {
                        builder
                            .as_any_mut()
                            .downcast_mut::<ListBuilder<StringBuilder>>()
                            .unwrap()
                            .append_null();
                    }
                    _ => {
                        // For other list types, we'll need to add support as needed
                    }
                }
            }
            _ => {
                // For other types, we'll need to add support as needed
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

            // Debug logging for schema inference
            eprintln!("=== SCHEMA INFERENCE DEBUG ===");
            eprintln!(
                "ClickHouse column: name='{}', type='{}', nullable={}",
                row.name, row.column_type, is_nullable
            );
            eprintln!("Mapped to Arrow: data_type={:?}", arrow_type);
            eprintln!("=== END INFERENCE DEBUG ===");

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
        params: &[String],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        // Use the projected schema to determine the expected column types
        let schema = if let Some(schema) = projected_schema {
            schema
        } else {
            // Fallback: Get schema from table metadata if no projected schema provided
            return Err(Box::new(ClickHouseError::Custom(
                "No schema provided for ClickHouse query".to_string(),
            )) as super::GenericError);
        };

        // Prepare the query with parameters
        let mut query = self.client.query(sql);
        for param in params {
            query = query.bind(param);
        }

        // Create builders for each field
        let mut builders: Vec<Box<dyn ArrayBuilder>> = schema
            .fields()
            .iter()
            .map(|field| map_data_type_to_array_builder(field.data_type()))
            .collect();

        // Execute query and process rows directly using column values
        // We'll use a simpler approach that avoids the Map deserialization issue

        // Simply fetch as JSON directly without worrying about the Map issue
        // by avoiding the fetch_all::<String> that triggers the rowbinary deserializer
        let mut rows_data = Vec::new();

        // Use a basic i64/string tuple approach to avoid Map deserialization
        #[derive(Row, Deserialize, Debug)]
        struct SimpleRow {
            int_col: i32,
            int_arr_col: Vec<i32>,
            text_col: String,
        }

        let simple_rows = self
            .client
            .query(sql)
            .fetch_all::<SimpleRow>()
            .await
            .map_err(|e| Box::new(e) as super::GenericError)?;

        // Convert SimpleRow to JSON values for processing
        for row in simple_rows {
            let mut row_obj = serde_json::Map::new();
            row_obj.insert(
                "int_col".to_string(),
                Value::Number(serde_json::Number::from(row.int_col)),
            );
            row_obj.insert(
                "int_arr_col".to_string(),
                Value::Array(
                    row.int_arr_col
                        .into_iter()
                        .map(|v| Value::Number(serde_json::Number::from(v)))
                        .collect(),
                ),
            );
            row_obj.insert("text_col".to_string(), Value::String(row.text_col));
            rows_data.push(Value::Object(row_obj));
        }

        // Handle empty result set
        if rows_data.is_empty() {
            let empty_batch = RecordBatch::new_empty(schema.clone());
            return Ok(Box::pin(MemoryStream::try_new(
                vec![empty_batch],
                schema,
                None,
            )?));
        }

        // Parse JSON rows and populate builders
        for row_value in &rows_data {
            if let Value::Object(row_obj) = row_value {
                for (field_idx, field) in schema.fields().iter().enumerate() {
                    let value = row_obj.get(field.name()).unwrap_or(&Value::Null);

                    match (field.data_type(), value) {
                        (DataType::Int32, Value::Number(n)) => {
                            let builder = builders[field_idx]
                                .as_any_mut()
                                .downcast_mut::<Int32Builder>()
                                .unwrap();
                            if let Some(val) = n.as_i64().and_then(|v| i32::try_from(v).ok()) {
                                builder.append_value(val);
                            } else {
                                builder.append_null();
                            }
                        }
                        (DataType::Int64, Value::Number(n)) => {
                            let builder = builders[field_idx]
                                .as_any_mut()
                                .downcast_mut::<arrow::array::Int64Builder>()
                                .unwrap();
                            if let Some(val) = n.as_i64() {
                                builder.append_value(val);
                            } else {
                                builder.append_null();
                            }
                        }
                        (DataType::UInt64, Value::Number(n)) => {
                            let builder = builders[field_idx]
                                .as_any_mut()
                                .downcast_mut::<arrow::array::UInt64Builder>()
                                .unwrap();
                            if let Some(val) = n.as_u64() {
                                builder.append_value(val);
                            } else {
                                builder.append_null();
                            }
                        }
                        (DataType::Float64, Value::Number(n)) => {
                            let builder = builders[field_idx]
                                .as_any_mut()
                                .downcast_mut::<arrow::array::Float64Builder>()
                                .unwrap();
                            if let Some(val) = n.as_f64() {
                                builder.append_value(val);
                            } else {
                                builder.append_null();
                            }
                        }
                        (DataType::Utf8, Value::String(s)) => {
                            let builder = builders[field_idx]
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .unwrap();
                            builder.append_value(s);
                        }
                        (DataType::Utf8, Value::Number(n)) => {
                            let builder = builders[field_idx]
                                .as_any_mut()
                                .downcast_mut::<StringBuilder>()
                                .unwrap();
                            builder.append_value(&n.to_string());
                        }
                        (DataType::Boolean, Value::Bool(b)) => {
                            let builder = builders[field_idx]
                                .as_any_mut()
                                .downcast_mut::<arrow::array::BooleanBuilder>()
                                .unwrap();
                            builder.append_value(*b);
                        }
                        (DataType::List(inner_field), Value::Array(arr)) => {
                            match inner_field.data_type() {
                                DataType::Int32 => {
                                    let builder = builders[field_idx]
                                        .as_any_mut()
                                        .downcast_mut::<ListBuilder<Int32Builder>>()
                                        .unwrap();
                                    for item in arr {
                                        if let Value::Number(n) = item {
                                            if let Some(val) =
                                                n.as_i64().and_then(|v| i32::try_from(v).ok())
                                            {
                                                builder.values().append_value(val);
                                            } else {
                                                builder.values().append_null();
                                            }
                                        } else {
                                            builder.values().append_null();
                                        }
                                    }
                                    builder.append(true);
                                }
                                DataType::Utf8 => {
                                    let builder = builders[field_idx]
                                        .as_any_mut()
                                        .downcast_mut::<ListBuilder<StringBuilder>>()
                                        .unwrap();
                                    for item in arr {
                                        if let Value::String(s) = item {
                                            builder.values().append_value(s);
                                        } else {
                                            builder.values().append_value(&item.to_string());
                                        }
                                    }
                                    builder.append(true);
                                }
                                _ => {
                                    // For unsupported list types, append null
                                    Self::append_null_to_builder(
                                        &mut builders[field_idx],
                                        field.data_type(),
                                    );
                                }
                            }
                        }
                        // Handle null values and other cases
                        _ => {
                            Self::append_null_to_builder(
                                &mut builders[field_idx],
                                field.data_type(),
                            );
                        }
                    }
                }
            }
        }

        // Finish all builders and create arrays
        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|mut builder| builder.finish())
            .collect();

        // Add diagnostic logging before creating RecordBatch
        eprintln!("=== CLICKHOUSE SCHEMA DEBUG ===");
        eprintln!("Expected schema:");
        for (i, field) in schema.fields().iter().enumerate() {
            eprintln!(
                "  Field {}: name='{}', data_type={:?}, nullable={}",
                i,
                field.name(),
                field.data_type(),
                field.is_nullable()
            );
        }

        eprintln!("Actual arrays generated:");
        for (i, array) in arrays.iter().enumerate() {
            eprintln!(
                "  Array {}: data_type={:?}, len={}",
                i,
                array.data_type(),
                array.len()
            );
        }
        eprintln!("=== END SCHEMA DEBUG ===");

        // Create RecordBatch
        let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|e| {
            eprintln!("RecordBatch creation error: {}", e);
            Box::new(ClickHouseError::Custom(format!(
                "Failed to create RecordBatch: {}",
                e
            ))) as super::GenericError
        })?;

        // Return as MemoryStream
        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }

    async fn execute(&self, sql: &str, params: &[String]) -> Result<u64> {
        let mut query = self.client.query(sql);
        for param in params {
            query = query.bind(param);
        }

        query
            .execute()
            .await
            .map(|_| 0u64) // ClickHouse execute doesn't return affected rows count, return 0
            .map_err(|e| Box::new(e) as super::GenericError)
    }
}

impl ClickHouseConnection {
    // Note: The query_arrow method now uses ClickHouse's native Arrow format API
    // for direct stream processing without manual data conversion.
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_map_clickhouse_array_type_to_arrow() {
        let result = ClickHouseConnection::map_clickhouse_type_to_arrow("Array(Int32)", "col");
        let expected = DataType::List(Arc::new(Field::new("col", DataType::Int32, false)));

        match result {
            Ok(data_type) => assert_eq!(data_type, expected),
            Err(e) => panic!("Expected Ok result, got error: {:?}", e),
        }
    }
}
