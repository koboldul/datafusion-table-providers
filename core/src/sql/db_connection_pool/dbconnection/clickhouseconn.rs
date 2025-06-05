#![cfg(feature = "clickhouse")]

use crate::sql::db_connection_pool::dbconnection::UnsupportedDataTypeSnafu;
use std::{any::Any, sync::Arc};

use crate::sql::db_connection_pool::dbconnection::{
    AsyncDbConnection, DbConnection, Error as DbConnectionError, GenericError, Result,
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_stream::stream;
use async_trait::async_trait;
use clickhouse::{error::Error as ClickHouseError, Client, Row};
use datafusion::{
    arrow::error::ArrowError, execution::SendableRecordBatchStream, sql::TableReference,
};
use futures::stream::StreamExt;
use snafu::prelude::*;

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

/* Removed trait impls for DbConnection<Pool, Query> for ClickHouseConnection */

/* Removed trait impls for AsyncDbConnection<Pool, Query> for ClickHouseConnection and all related methods. */
