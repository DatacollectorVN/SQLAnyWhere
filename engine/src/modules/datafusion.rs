use std::path::Path;

use datafusion::execution::context::SessionContext;
use datafusion::parquet::file;
use datafusion::prelude::{ParquetReadOptions, DataFrame, CsvReadOptions};
use datafusion::error::Result;
use datafusion::arrow::array::RecordBatch;
use crate::modules::utils;

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

    fn get_parquet_option<'a>(&self, option: Option<ParquetReadOptions<'a>>) -> ParquetReadOptions<'a> {
        option.unwrap_or_else(ParquetReadOptions::default)
    }

    fn get_csv_option<'a>(&self, option: Option<CsvReadOptions<'a>>) -> CsvReadOptions<'a> {
        option.unwrap_or_else(CsvReadOptions::default)
    }

    pub async fn execute_sql(&self, stm: &str) -> Result<DataFrame> {
        let df: DataFrame = self.client.sql(stm).await?;
        Ok(df)
    }

    pub async fn register_file(&self, file_path: &str, option: Option<FileFormatOptions<'_>>) -> Result<(), Box<dyn std::error::Error>> {
        let file_extension: &str = &utils::extract_path(file_path, Path::extension, "extension")
            .unwrap()
            .to_lowercase();

        let file_name: &str = &utils::extract_path(file_path, Path::file_stem, "file stem")
            .unwrap()
            .to_lowercase();

        match (option, file_extension) {
            (Some(FileFormatOptions::Parquet(parquet_options)), "parquet") => {
                let target_option: ParquetReadOptions<'_> = self.get_parquet_option(Some(parquet_options));
                self.client
                    .register_parquet(file_name, file_path, target_option).await?;
            }
            (Some(FileFormatOptions::Csv(csv_options)), "csv") => {
                let target_option: CsvReadOptions<'_> = self.get_csv_option(Some(csv_options));
                self.client
                    .register_csv(file_name, file_path, target_option).await?;
            }
            _ => {
                panic!("No file format options provided!");
            }
        }
        Ok(())
    }
}
