#![cfg(feature = "clickhouse")]

use crate::sql::{
    db_connection_pool::clickhousepool::{ClickHouseConnectionPool, DynClickHouseConnectionPool},
    sql_provider_datafusion::{self, SqlTable},
};
use async_trait::async_trait;
use clickhouse::Client;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::execution_plan::{Boundedness, EmissionType},
    physical_plan::{ExecutionPlan, Partitioning, PlanProperties},
    sql::TableReference,
};
use std::{any::Any, sync::Arc};

/********************************
    Table provider
*********************************/

#[derive(Clone)]
pub struct ClickHouseTableProvider {
    pool: Arc<ClickHouseConnectionPool>,
    pub(crate) base_table: SqlTable<Client, String>,
    schema: SchemaRef,
}

impl std::fmt::Debug for ClickHouseTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseTable")
            .field("table_reference", &self.base_table)
            .field("schema", &self.schema)
            .finish()
    }
}

impl ClickHouseTableProvider {
    pub async fn new(
        pool: &Arc<ClickHouseConnectionPool>,
        table_reference: TableReference,
    ) -> Result<Self, sql_provider_datafusion::Error> {
        let dyn_pool = Arc::clone(pool) as Arc<DynClickHouseConnectionPool>;
        let base_table = SqlTable::new("clickhouse", &dyn_pool, table_reference).await?;
        let schema = base_table.schema();

        Ok(Self {
            pool: Arc::clone(pool),
            base_table,
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for ClickHouseTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Project the schema based on the projection
        let projected_schema = datafusion::physical_plan::project_schema(&self.schema, projection)?;

        // Return an empty MemoryStream for the projected schema
        use arrow::record_batch::RecordBatch;

        let batches: Vec<RecordBatch> = Vec::new();

        Ok(Arc::new(ClickHouseSQLExec::new(projected_schema, batches)))
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // TODO: Implement actual filter pushdown analysis
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            _filters.len()
        ])
    }
}

/********************************
    Execution Plan
*********************************/

// A simple ExecutionPlan that stores data needed to create streams
struct ClickHouseSQLExec {
    schema: SchemaRef,
    batches: Arc<Vec<arrow::record_batch::RecordBatch>>,
    plan_properties: PlanProperties,
}

impl ClickHouseSQLExec {
    fn new(schema: SchemaRef, batches: Vec<arrow::record_batch::RecordBatch>) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            batches: Arc::new(batches),
            plan_properties,
        }
    }
}

impl std::fmt::Debug for ClickHouseSQLExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamExec")
            .field("schema", &self.schema)
            .finish()
    }
}

// Safety: StreamExec is Send and Sync if its fields are
unsafe impl Send for ClickHouseSQLExec {}
unsafe impl Sync for ClickHouseSQLExec {}

use datafusion::physical_plan::DisplayAs;
use std::fmt;

impl DisplayAs for ClickHouseSQLExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default => write!(f, "StreamExec"),
            _ => write!(f, "StreamExec (unimplemented format)"),
        }
    }
}

impl ExecutionPlan for ClickHouseSQLExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "StreamExec"
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.plan_properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
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
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        use datafusion::physical_plan::memory::MemoryStream;

        let batches = (*self.batches).clone();
        let stream: SendableRecordBatchStream =
            Box::pin(MemoryStream::try_new(batches, self.schema.clone(), None)?);

        Ok(stream)
    }

    fn statistics(&self) -> Result<datafusion::common::Statistics, DataFusionError> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }
}
