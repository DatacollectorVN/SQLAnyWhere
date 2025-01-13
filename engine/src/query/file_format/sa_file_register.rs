use datafusion::execution::context::SessionContext;
use datafusion::error::Result;
use datafusion::prelude::DataFrame;
use std::future::Future;

pub trait SaFileRegister {
    type SaCustomOptions;
    fn sa_get_options<'a>(&self) -> Self::SaCustomOptions;
    fn sa_register(&self, client: &SessionContext, file_name: &str, file_path: &str) -> impl Future<Output = Result<()>> + Send;
    fn sa_read(&self, client: &SessionContext, file_path: &str) -> impl Future<Output = Result<DataFrame>> + Send;
    // fn sa_get_schema(&self, client: &SessionContext, file_path: &str, data_type: Option<DataType>) -> impl Future<Output = Result<Schema>> + Send;
}