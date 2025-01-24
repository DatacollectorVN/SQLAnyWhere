use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;
use datafusion::error::Result;
use object_store::ObjectStore;
use std::sync::Arc;
use datafusion::common::DFSchema;
use crate::object_storage::storage::SaStorage;
use datafusion::execution::SessionState;
use url::Url;


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

    pub async fn execute_sql(&self, stm:&str ) -> Result<DataFrame> {
        self.ctx.sql(stm).await
    }


    pub fn register_object_store(&self, url: &Url, object_store: Arc<dyn ObjectStore>) {
        self.ctx.runtime_env().register_object_store(url, object_store);
    }

    pub async fn register_sa_storage(&self, sa_storage: Arc<dyn SaStorage>) -> Result<()>{
        self.ctx.register_table(sa_storage.get_file_url(), sa_storage.get_table_provider())?;
        Ok(())
    }

    pub async fn get_schema(&self, table_name: &str) -> Result<DFSchema> {
        Ok(
            self.ctx
                .table(table_name)
                .await?
                .schema()
                .clone()
        )
    }

    pub async fn display_schema(&self, table_name: &str) -> Result<()> {
        for field in self.get_schema(table_name).await?.fields() {
            println!("{} - {}", field.name(), field.data_type())
        }
        Ok(())
    }
}
