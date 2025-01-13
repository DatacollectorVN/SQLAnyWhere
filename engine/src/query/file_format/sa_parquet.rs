use datafusion::execution::context::SessionContext;
use datafusion::error::Result;
use crate::query::file_format::sa_file_register::SaFileRegister;
pub use datafusion::prelude::{ParquetReadOptions, DataFrame};

impl<'a> SaFileRegister for ParquetReadOptions<'a> {
    type SaCustomOptions = ParquetReadOptions<'a>;

    fn sa_get_options(&self) -> Self::SaCustomOptions{
        Some(self.clone()).unwrap_or_else(ParquetReadOptions::default)
    }

    async fn sa_register(&self, client: &SessionContext, table_name: &str, file_path: &str) -> Result<()> {
        client.register_parquet(table_name, file_path, self.sa_get_options()).await
    }

    async fn sa_read(&self, client: &SessionContext, file_path: &str) -> Result<DataFrame> {
        client.read_parquet(file_path, self.sa_get_options()).await
    }
}