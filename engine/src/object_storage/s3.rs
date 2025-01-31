use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use url::Url;
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
use object_store::aws::{AmazonS3Builder, AmazonS3};
use object_store::ObjectStore;
use regex::Regex;
use crate::object_storage::storage::SaStorage;
use crate::datafusion::SaDataFusion;


#[derive(Debug, Clone)]
pub struct SaS3 {
    s3_bucket: String,
    s3_src_key: String,
    s3_file: String,
    file_url: String,
    table_provider: Option<Arc<dyn TableProvider>>,
    object_store: Option<Arc<dyn ObjectStore>>
}


impl Default for SaS3 {
    fn default() -> Self {
        SaS3 {
            s3_bucket: String::new(),
            s3_src_key: String::new(),
            s3_file: String::new(),
            file_url: String::new(),
            table_provider: None,
            object_store: None
        }
    }
}


impl SaS3 {
    pub const PROTOCAL: &str = "s3";

    pub fn get_s3_src_key(&self) -> String {
        self.s3_src_key.clone()
    }

    pub fn get_s3_file(&self) -> String {
        self.s3_file.clone()
    }

    pub fn get_s3_bucket(&self) -> String {
        self.s3_bucket.clone()
    }

    pub fn new(
        s3_bucket: &str,
        s3_src_key: &str,
        s3_file: &str
    ) -> Self {
        let file_url: String = format!("{}://{}/{}/{}", Self::PROTOCAL, s3_bucket, s3_src_key, s3_file);

        Self {
            s3_bucket: s3_bucket.to_string(),
            s3_src_key: s3_src_key.to_string(),
            s3_file: s3_file.to_string(),
            file_url,
            ..Default::default()
        }
    }

    pub fn new_from_s3_uri(s3_uri: &str) -> Self {
        let pattern: &str = r"s3://([^/]+)/(.+)/([^/]+)$";
        let re: Regex = Regex::new(pattern).unwrap();

        match re.captures(s3_uri) {
            Some(captures) => {
                let default_value: &str = "";
                let s3_bucket: &str = captures.get(1).map_or(default_value, |m| m.as_str());
                let s3_src_key: &str = captures.get(2).map_or(default_value, |m| m.as_str());
                let s3_file: &str = captures.get(3).map_or(default_value, |m| m.as_str());
                Self {
                    s3_bucket: s3_bucket.to_string(),
                    s3_src_key: s3_src_key.to_string(),
                    s3_file: s3_file.to_string(),
                    file_url: s3_uri.to_string(),
                    ..Default::default()
                }
            },
            _ => {
                panic!("[new_from_s3_uri][Exception]: Could not parse the S3 URI: {}", s3_uri);
            }
        }
    }

    pub async fn init_table_provider(
        mut self,
        s3_region: &str,
        sa_datafusion: &SaDataFusion,
        file_format: Arc<dyn FileFormat>,
        infer_schema: Option<bool>,
    ) -> Result<Self>  {
        let is_infer_schema: bool = infer_schema.unwrap_or(true);
        let s3: AmazonS3 = AmazonS3Builder::from_env() // extract credential inforation by OS env
            .with_region(s3_region) // Replace with your AWS region
            .with_bucket_name(self.s3_bucket.clone()) // Replace with your bucket name
            .build()?;

        let object_store: Arc<dyn ObjectStore> = Arc::new(s3);
        sa_datafusion.register_object_store(&Url::parse(&self.file_url).unwrap(), object_store);
        let listing_table_url: ListingTableUrl = ListingTableUrl::parse(self.file_url.clone())?;
        let listing_options: ListingOptions = ListingOptions::new(file_format);

        let schema: Arc<Schema> = if is_infer_schema {
            // Auto inference
            listing_options
                .infer_schema(&sa_datafusion.get_session_state(), &listing_table_url)
                .await?
        } else {
            // Create a schema with all columns as DataType::Utf8 (string)
            let inferred_schema = listing_options
                .infer_schema(&sa_datafusion.get_session_state(), &listing_table_url)
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


impl SaStorage for SaS3 {
    fn get_table_provider(&self) -> Arc<dyn TableProvider> {
        self.table_provider.clone().unwrap()
    }

    fn get_file_url(&self) -> String {
        self.file_url.clone()
    }

    fn get_protocal(&self) -> String {
        Self::PROTOCAL.to_string()
    }

    fn get_object_store(&self) -> Option<Arc<dyn ObjectStore>>{
        Some(self.object_store.clone().unwrap())
    }
}


#[async_trait]
impl TableProvider for SaS3 {
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