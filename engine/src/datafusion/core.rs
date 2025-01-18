use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;
use datafusion::error::Result;
use std::sync::Arc;
use datafusion::common::DFSchema;
use crate::object_storage::storage::SaStorage;
use datafusion::execution::SessionState;
use std::error::Error;


pub struct SaDataFusion {
    pub ctx: SessionContext,
}


impl SaDataFusion {
    pub fn new() -> Self {
        Self {ctx: SessionContext::new()}
    }

    pub fn get_session_state(&self) -> SessionState {
        self.ctx.state()
    }
    pub async fn execute_sql(&self, stm: &str) -> Result<DataFrame> {
        self.ctx.sql(stm).await
    }

    pub async fn register_storage(&self, sa_object: Arc<dyn SaStorage>) -> Result<()>{
        self.ctx.register_table(sa_object.get_table_name(), sa_object.get_table_provider())?;
        Ok(())
    }

    pub async fn get_schema_in_table(&self, table_name: &str) -> Result<DFSchema, Box<dyn Error>> {
        Ok(
            self.ctx
                .table(table_name)
                .await?
                .schema()
                .clone()
        )
    }
}
