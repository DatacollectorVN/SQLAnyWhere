use datafusion::execution::context::SessionContext;
use datafusion::error::Result;
pub use crate::sa_datafusion::sa_file_format::SaFileRegister;
pub use datafusion::prelude::{DataFrame, CsvReadOptions};


impl<'a> SaFileRegister for CsvReadOptions<'a> {
    type SaCustomOptions = CsvReadOptions<'a>;

    fn sa_get_options(&self) -> Self::SaCustomOptions{
        Some(self.clone()).unwrap_or_else(CsvReadOptions::default)
    }

    async fn sa_register(&self, client: &SessionContext, table_name: &str, file_path: &str) -> Result<()> {
        client.register_csv(table_name, file_path, self.sa_get_options()).await
    }

    async fn sa_read(&self, client: &SessionContext, file_path: &str) -> Result<DataFrame> {
        client.read_csv(file_path, self.sa_get_options()).await
    }
}