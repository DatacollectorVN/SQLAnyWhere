use std::sync::Arc;
use datafusion::datasource::TableProvider;


pub trait SaStorage {
    fn get_table_provider(&self) -> Arc<dyn TableProvider>;
    fn get_file_url(&self) -> String;
    fn get_table_name(&self) -> String;
}