pub mod sa_file_options;
pub mod sa_file_format;
use crate::sa_datafusion::sa_file_options::SaFileOptions;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::DataFrame;
use datafusion::error::Result;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::common::DFSchema;
use std::error::Error;


pub struct SaDataFusion {
    pub client: SessionContext,
}

impl SaDataFusion {
    pub fn new() -> Self {
        Self {
            client: SessionContext::new(),
        }
    }

    pub async fn execute_sql(&self, stm: &str) -> Result<DataFrame> {
        self.client.sql(stm).await
    }

    pub async fn register(&self, file_path: &str, options: Option<SaFileOptions<'_>>) -> Result<()> {
        match options {
            Some(sa_file_options) => sa_file_options.register(&self.client, file_path).await,
            _ => panic!("[SADataFusion][register][Exception]: No file format options provided!")
        }
    }

    fn convert_dtype(&self, type_str: &str) -> Result<DataType>{
        match type_str.to_lowercase().as_str() {
            "string" => Ok(DataType::Utf8),
            "float" => Ok(DataType::Float64),
            _ => Err(DataFusionError::Plan(format!("Unsupported data type: {}", type_str)))
        }
    }

    pub async fn get_schema_in_table(&self, table_name: &str) -> Result<DFSchema, Box<dyn Error>> {
        Ok(
            self.client
                .table(table_name)
                .await?
                .schema()
                .clone()
        )
    }

    pub async fn read(&self, file_path: &str, options: Option<&SaFileOptions<'_>>) -> Result<DataFrame> {
        match options {
            Some(sa_file_options) => sa_file_options.read(&self.client, file_path).await,
            _ => panic!("[SADataFusion][read][Exception]: No file format options provided!")
        }
    }

    pub async fn get_cast_schema(&self, file_path: &str, cast_type: &str, options: Option<&SaFileOptions<'_>>) -> Result<Schema>{
        let table_schema: Schema = Schema::new(
            self.read(file_path, options).await?.schema()
            .fields()
            .iter()
            .map(
                |field| {
                    Field::new(
                        field.name(),
                        self.convert_dtype(
                            cast_type
                        ).unwrap(),
                        field.is_nullable()
                    )
                }
            ).collect::<Vec<_>>(),
        );
        Ok(table_schema)
    }
}
