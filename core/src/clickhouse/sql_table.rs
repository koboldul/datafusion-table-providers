#![cfg(feature = "clickhouse")]

use crate::sql::{
    db_connection_pool::clickhousepool::{ClickHouseConnectionPool, DynClickHouseConnectionPool},
    sql_provider_datafusion::{self, get_stream, to_execution_error, SqlExec, SqlTable},
};
use async_trait::async_trait;
use clickhouse::Client;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties,
    },
    sql::TableReference,
};
use futures::TryStreamExt;
use std::{any::Any, fmt, sync::Arc};

/********************************
    Table provider
*********************************/

pub struct ClickHouseTable {
    pool: Arc<ClickHouseConnectionPool>,
    pub(crate) base_table: SqlTable<Client, String>,
}

impl std::fmt::Debug for ClickHouseTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseTable")
            .field("base_table", &self.base_table)
            .finish()
    }
}

impl ClickHouseTable {
    pub async fn new(
        pool: &Arc<ClickHouseConnectionPool>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self, sql_provider_datafusion::Error> {
        let dyn_pool = Arc::clone(pool) as Arc<DynClickHouseConnectionPool>;
        let base_table = SqlTable::new("clickhouse", &dyn_pool, table_reference).await?;

        Ok(Self {
            pool: Arc::clone(pool),
            base_table,
        })
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let sql = self.base_table.scan_to_sql(projections, filters, limit)?;
        Ok(Arc::new(ClickHouseSQLExec::new(
            projections,
            schema,
            Arc::clone(&self.pool),
            sql,
        )?))
    }
}

#[async_trait]
impl TableProvider for ClickHouseTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.base_table.schema()
    }

    fn table_type(&self) -> TableType {
        self.base_table.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.base_table.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(projection, &self.schema(), filters, limit)
    }
}

impl fmt::Display for ClickHouseTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHouseTable {}", self.base_table.name())
    }
}

/********************************
    Execution Plan
*********************************/

struct ClickHouseSQLExec {
    base_exec: SqlExec<Client, String>,
}

impl ClickHouseSQLExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        pool: Arc<ClickHouseConnectionPool>,
        sql: String,
    ) -> DataFusionResult<Self> {
        let base_exec = SqlExec::new(projections, schema, pool, sql)?;

        Ok(Self { base_exec })
    }

    fn sql(&self) -> sql_provider_datafusion::Result<String> {
        self.base_exec.sql()
    }
}

impl std::fmt::Debug for ClickHouseSQLExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "ClickHouseSQLExec sql={sql}")
    }
}

impl DisplayAs for ClickHouseSQLExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "ClickHouseSQLExec sql={sql}")
    }
}

impl ExecutionPlan for ClickHouseSQLExec {
    fn name(&self) -> &'static str {
        "ClickHouseSQLExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.base_exec.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.base_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.base_exec.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = self.sql().map_err(to_execution_error)?;
        tracing::debug!("ClickHouseSQLExec sql: {sql}");

        let fut = get_stream(self.base_exec.clone_pool(), sql, Arc::clone(&self.schema()));

        let stream = futures::stream::once(fut).try_flatten();
        let schema = Arc::clone(&self.schema());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
