use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use std::path::Path;
use std::io::{
    self,
    Error,
    ErrorKind
};
use std::ffi::OsStr;
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
use datafusion::execution::context::SessionState;
use datafusion::common::Result;
use datafusion::arrow::datatypes::{
    Schema,
    SchemaRef
};
use datafusion_expr::{
    TableType,
    Expr
};
use datafusion::physical_plan::ExecutionPlan;
use crate::object_storage::storage::SaStorage;


#[derive(Debug)]
pub struct SaLocalStorage {
    file_url: String,
    table_provider: Arc<dyn TableProvider>,
    table_name: String
}


impl SaLocalStorage {
    const PREFIX_URL: &str = "file://";

    fn extract_path<P, F>(
        path: P,
        extract_path_fn: F,
        mode: &str
    ) -> io::Result<String>
    where
        P: AsRef<Path>,
        F: Fn(&Path) -> Option<&OsStr>
    {
        let path_ref: &Path = path.as_ref();
        match extract_path_fn(path_ref) {
            Some(os_str) => os_str
                .to_str()
                .map(|s| s.to_string())
                .ok_or_else(
                    || {
                        Error::new(ErrorKind::InvalidData, format!("Invalid UTF-8 in {}", mode),)
                    }
                ),
            None => Err(Error::new(ErrorKind::InvalidData, format!("Path has no valid {}", mode)))
        }
    }

    pub fn copy_file<P: AsRef<Path>>(src: P, dest: P) -> io::Result<u64> {
        fs::copy(src, dest)
    }

    pub fn move_file<P: AsRef<Path>>(src: P, dest: P) -> io::Result<()> {
        fs::copy(&src, &dest)?;
        fs::remove_file(src)
    }

    pub fn get_file_name<P: AsRef<Path>>(path: P) -> io::Result<String> {
        Self::extract_path(path, Path::file_name, "file name")
    }

    pub fn get_file_stem<P: AsRef<Path>>(path: P) -> io::Result<String> {
        Self::extract_path(path, Path::file_stem, "file stem")
    }

    pub fn get_file_extension<P: AsRef<Path>>(path: P) -> io::Result<String> {
        Self::extract_path(path, Path::extension, "extension")
    }

    pub async fn new(file_path: &str, session_state: &SessionState, file_format: Arc<dyn FileFormat>,) -> Result<Self> {
        let file_url: String = format!("{}{}", Self::PREFIX_URL, file_path);
        let listing_options: ListingOptions = ListingOptions::new(file_format);
        let listing_table_url: ListingTableUrl = ListingTableUrl::parse(file_url.clone())?;
        let infer_schema: Arc<Schema> = listing_options.infer_schema(session_state, &listing_table_url).await?;
        let listing_table_config: ListingTableConfig = ListingTableConfig::new(listing_table_url)
            .with_listing_options(listing_options)
            .with_schema(infer_schema);

        let table_provider: Arc<ListingTable> = Arc::new(ListingTable::try_new(listing_table_config)?);
        let table_name: String = Self::get_file_stem(file_path).unwrap();
        Ok(Self{file_url, table_provider, table_name})
    }
}


impl SaStorage for SaLocalStorage {
    fn get_table_provider(&self) -> Arc<dyn TableProvider> {
        self.table_provider.clone()
    }

    fn get_file_url(&self) -> String {
        self.file_url.clone()
    }

    fn get_table_name(&self) -> String {
        self.table_name.clone()
    }
}


#[async_trait]
impl TableProvider for SaLocalStorage {
    fn as_any(&self) -> &dyn Any {
        &self.table_provider
    }

    /// Returns the schema of the table
    fn schema(&self) -> SchemaRef {
        self.table_provider.schema()
    }

    /// Returns the type of the table (e.g., Base or View)
    fn table_type(&self) -> TableType {
        self.table_provider.table_type()
    }

    /// Creates a logical plan for scanning the table
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.table_provider.scan(state, projection, filters, limit).await
    }
}

