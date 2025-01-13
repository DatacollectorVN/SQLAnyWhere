use datafusion::{execution::context::SessionContext, prelude::DataFrame};
use datafusion::error::DataFusionError;
use std::path::Path;
use crate::sa_datafusion::sa_file_format::{
    SaFileRegister,
    sa_csv::CsvReadOptions,
    sa_parquet::ParquetReadOptions,

};
use crate::sa_helper;
use datafusion::error::Result;

#[derive(Clone)]
pub enum SaFileOptions<'a> {
    Parquet(ParquetReadOptions<'a>),
    Csv(CsvReadOptions<'a>),
    // Extend with other formats as needed
}

impl<'a> SaFileOptions<'a> {
    pub async fn register(&self, client: &SessionContext, file_path: &str) -> Result<(), DataFusionError> {
        let file_name: &str = &sa_helper::extract_path(file_path, Path::file_stem, "file stem")
            .unwrap()
            .to_lowercase();

        match self {
            SaFileOptions::Parquet(options) => options.sa_register(client, file_name, file_path).await,
            SaFileOptions::Csv(options) => options.sa_register(client, file_name, file_path).await,
        }
    }

    pub async fn read(&self, client: &SessionContext, file_path: &str) -> Result<DataFrame, DataFusionError> {
        match self {
            SaFileOptions::Parquet(options) => options.sa_read(client, file_path).await,
            SaFileOptions::Csv(options) => options.sa_read(client, file_path).await,
        }
    }
}


