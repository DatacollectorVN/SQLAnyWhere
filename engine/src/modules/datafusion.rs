use std::path::Path;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{ParquetReadOptions, DataFrame, CsvReadOptions};
use datafusion::error::Result;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::common::DFSchema;
use std::error::Error;
use crate::modules::utils;


pub trait Registerable {
    type CustomOptions;
    fn get_options<'a>(&self) -> Self::CustomOptions;
    fn register(&self, client: &SessionContext, file_name: &str, file_path: &str) -> impl std::future::Future<Output = Result<()>> + Send;
}

impl<'a> Registerable for ParquetReadOptions<'a> {
    type CustomOptions = ParquetReadOptions<'a>;

    fn get_options(&self) -> Self::CustomOptions{
        Some(self.clone()).unwrap_or_else(ParquetReadOptions::default)
    }

    async fn register(&self, client: &SessionContext, file_name: &str, file_path: &str) -> Result<()> {
        client.register_parquet(file_name, file_path, self.get_options()).await
    }
}

impl<'a> Registerable for CsvReadOptions<'a> {
    type CustomOptions = CsvReadOptions<'a>;

    fn get_options(&self) -> Self::CustomOptions{
        Some(self.clone()).unwrap_or_else(CsvReadOptions::default)
    }

    async fn register(&self, client: &SessionContext, file_name: &str, file_path: &str) -> Result<()> {
        client.register_csv(file_name, file_path, self.get_options()).await
    }
}

impl<'a> FileFormatOptions<'a> {
    pub async fn register(&self, client: &SessionContext, file_name: &str, file_path: &str) -> Result<()> {
        match self {
            FileFormatOptions::Parquet(options) => options.register(client, file_name, file_path).await,
            FileFormatOptions::Csv(options) => options.register(client, file_name, file_path).await,
        }
    }
}


pub enum FileFormatOptions<'a> {
    Parquet(ParquetReadOptions<'a>),
    Csv(CsvReadOptions<'a>),
    // Extend with other formats as needed
}


pub struct SADataFusion {
    pub client: SessionContext,
}

impl SADataFusion {
    pub fn new() -> Self {
        Self {
            client: SessionContext::new(),
        }
    }

    pub async fn execute_sql(&self, stm: &str) -> Result<DataFrame> {
        let df: DataFrame = self.client.sql(stm).await?;
        Ok(df)
    }

    pub async fn register(&self, file_path: &str, options: Option<FileFormatOptions<'_>>) -> Result<()> {
        let file_name: &str = &utils::extract_path(file_path, Path::file_stem, "file stem")
            .unwrap()
            .to_lowercase();

        match options {
            Some(file_format_options) => file_format_options.register(&self.client, file_name, file_path).await,
            _ => panic!("[SADataFusion][register_file][Exception]: No file format options provided!")
        }
    }

    fn convert_dtype(&self, type_str: &str) -> Result<DataType>{
        match type_str.to_lowercase().as_str() {
            "string" => Ok(DataType::Utf8),
            "float" => Ok(DataType::Float64),
            _ => Err(DataFusionError::Plan(format!("Unsupported data type: {}", type_str)))
        }
    }

    pub async fn get_schema_in_table(&self, table_name: &str) -> Result<(DFSchema), Box<dyn Error>> {
        Ok(
            self.client
                .table(table_name)
                .await?
                .schema()
                .clone()
        )
    }

    pub async fn cast_all_columns(&self, file_path: &str, type_str: &str) -> Result<Schema> {
        let file_extension: &str = &utils::extract_path(file_path, Path::extension, "extension")
            .unwrap()
            .to_lowercase();

        let string_schema: Schema = match file_extension {
            "parquet" => {
                Schema::new(
                    self.client.read_parquet(file_path, ParquetReadOptions::default()).await?.schema()
                    .fields()
                    .iter()
                    .map(|field| {
                        Field::new(field.name(), self.convert_dtype(type_str).unwrap(), field.is_nullable())
                    })
                    .collect::<Vec<_>>(),
                )
            },

            "csv" => {
                Schema::new(
                    self.client.read_csv(file_path, CsvReadOptions::default()).await?.schema()
                    .fields()
                    .iter()
                    .map(|field| {
                        Field::new(field.name(), self.convert_dtype(type_str).unwrap(), field.is_nullable())
                    })
                    .collect::<Vec<_>>(),
                )
            },
            _ => panic!("[SADataFusion][read_option][Exception]: No file format options provided!"),
        };
        Ok(string_schema)
    }
}
