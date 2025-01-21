use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use std::path::Path;
use std::io;
use std::fs;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::{
    ListingOptions,
    ListingTable,
    ListingTableConfig,
    ListingTableUrl
};
use datafusion::datasource::file_format::FileFormat;
use datafusion::common::Result;
use datafusion::arrow::datatypes::{
    Schema,
    SchemaRef,
    Field,
    DataType
};
use datafusion_expr::{
    TableType,
    Expr
};
use datafusion::physical_plan::ExecutionPlan;
use crate::object_storage::storage::SaStorage;
use crate::datafusion::SaDataFusion;
use crate::object_storage::utils;
use object_store::ObjectStore;


#[derive(Debug, Clone)]
pub struct SaLocalStorage {
    file_url: String,
    table_provider: Option<Arc<dyn TableProvider>>,
}


impl Default for SaLocalStorage {
    fn default() -> Self {
        SaLocalStorage {
            file_url: String::new(),
            table_provider: None,
        }
    }
}


impl SaLocalStorage {
    const PROTOCAL: &str = "file";

    pub fn copy_file<P: AsRef<Path>>(src: P, dest: P) -> io::Result<u64> {
        fs::copy(src, dest)
    }

    pub fn move_file<P: AsRef<Path>>(src: P, dest: P) -> io::Result<()> {
        fs::copy(&src, &dest)?;
        fs::remove_file(src)
    }

    pub fn delete_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
        fs::remove_file(path)
    }

    pub fn get_file_name<P: AsRef<Path>>(path: P) -> io::Result<String> {
        utils::extract_path(path, Path::file_name, "file name")
    }

    pub fn get_file_stem<P: AsRef<Path>>(path: P) -> io::Result<String> {
        utils::extract_path(path, Path::file_stem, "file stem")
    }

    pub fn get_file_extension<P: AsRef<Path>>(path: P) -> io::Result<String> {
        utils::extract_path(path, Path::extension, "extension")
    }

    pub fn new(file_path: &str) -> Self {
        let file_url: String = format!("{}://{}", Self::PROTOCAL, file_path);

        Self {
            file_url: file_url,
            ..Default::default()
        }
    }

    pub async fn init_table_provider(mut self, sa_dafusion: &SaDataFusion, file_format: Arc<dyn FileFormat>, is_infer_schema: Option<bool>) -> Result<Self> {
        let is_infer_schema: bool = is_infer_schema.unwrap_or(true);
        let listing_options: ListingOptions = ListingOptions::new(file_format);
        let listing_table_url: ListingTableUrl = ListingTableUrl::parse(self.file_url.clone())?;
        let schema: Arc<Schema> = if is_infer_schema {
            // Auto inference
            listing_options
                .infer_schema(&sa_dafusion.get_session_state(), &listing_table_url)
                .await?
        } else {
            // Create a schema with all columns as DataType::Utf8 (string)
            let inferred_schema = listing_options
                .infer_schema(&sa_dafusion.get_session_state(), &listing_table_url)
                .await?;

            let fields_as_string: Vec<Field> = inferred_schema
                .fields()
                .iter()
                .map(|field| Field::new(field.name(), DataType::Utf8, field.is_nullable()))
                .collect();

            Arc::new(Schema::new(fields_as_string))
        };

        let listing_table_config: ListingTableConfig = ListingTableConfig::new(listing_table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table_provider: Arc<ListingTable> = Arc::new(ListingTable::try_new(listing_table_config)?);
        self.table_provider = Some(table_provider);
        Ok(self)
    }
}


impl SaStorage for SaLocalStorage {
    fn get_table_provider(&self) -> Arc<dyn TableProvider> {
        self.table_provider.clone().unwrap()
    }

    fn get_file_url(&self) -> String {
        self.file_url.clone()
    }

    fn get_protocal(&self) -> String {
        Self::PROTOCAL.to_string()
    }

    fn get_object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        None
    }
}


#[async_trait]
impl TableProvider for SaLocalStorage {
    fn as_any(&self) -> &dyn Any {
        &self.table_provider
    }

    /// Returns the schema of the table
    fn schema(&self) -> SchemaRef {
        self.table_provider.clone().unwrap().schema()
    }

    /// Returns the type of the table (e.g., Base or View)
    fn table_type(&self) -> TableType {
        self.table_provider.clone().unwrap().table_type()
    }

    /// Creates a logical plan for scanning the table
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.table_provider.clone().unwrap().scan(state, projection, filters, limit).await
    }
}

