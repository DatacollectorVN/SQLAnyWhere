use std::sync::Arc;
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::{
    ListingOptions,
    ListingTable,
    ListingTableConfig,
    ListingTableUrl
};
use object_store::ObjectStore;


pub trait SaStorage {
    fn get_protocal(&self) -> String;
    fn get_table_provider(&self) -> Arc<dyn TableProvider>;
    fn get_file_url(&self) -> String;
    fn get_table_name(&self) -> String;
    fn get_object_store(&self) -> Option<Arc<dyn ObjectStore>>;
}