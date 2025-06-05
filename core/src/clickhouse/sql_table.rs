#![cfg(feature = "clickhouse")]

use crate::sql::db_connection_pool::{clickhousepool::ClickHouseConnectionPool, DbConnectionPool};
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, SendableRecordBatchStream},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::execution_plan::{Boundedness, EmissionType},
    physical_plan::{project_schema, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties},
    sql::TableReference,
};
use std::{any::Any, sync::Arc};

#[derive(Clone)]
pub struct ClickHouseTable {
    client: Arc<clickhouse::Client>,
    table_reference: TableReference,
    schema: SchemaRef,
}

impl std::fmt::Debug for ClickHouseTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseTable")
            .field("table_reference", &self.table_reference)
            .field("schema", &self.schema)
            .finish()
    }
}

impl ClickHouseTable {
    pub fn new(
        client: Arc<clickhouse::Client>,
        table_reference: TableReference,
        schema: SchemaRef,
    ) -> Self {
        Self {
            client,
            table_reference,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for ClickHouseTable {
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
        use datafusion::execution::SendableRecordBatchStream;
        use datafusion::physical_plan::memory::MemoryStream;

        let batches: Vec<RecordBatch> = Vec::new();
        let stream: SendableRecordBatchStream = Box::pin(MemoryStream::try_new(
            batches,
            projected_schema.clone(),
            None,
        )?);

        Ok(Arc::new(StreamExec::new(projected_schema, stream)))
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

// A simple ExecutionPlan that wraps a SendableRecordBatchStream
struct StreamExec {
    schema: SchemaRef,
    stream: SendableRecordBatchStream,
    plan_properties: PlanProperties,
}

impl StreamExec {
    fn new(schema: SchemaRef, stream: SendableRecordBatchStream) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            stream,
            plan_properties,
        }
    }
}

impl std::fmt::Debug for StreamExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamExec")
            .field("schema", &self.schema)
            .finish()
    }
}

// Safety: StreamExec is Send and Sync if its fields are
unsafe impl Send for StreamExec {}
unsafe impl Sync for StreamExec {}

use datafusion::physical_plan::DisplayAs;
use std::fmt;

impl DisplayAs for StreamExec {
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

impl ExecutionPlan for StreamExec {
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
        Err(DataFusionError::NotImplemented(
            "StreamExec::execute needs refinement - stream should likely be created here"
                .to_string(),
        ))
    }

    fn statistics(&self) -> Result<datafusion::common::Statistics, DataFusionError> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema))
    }
}
