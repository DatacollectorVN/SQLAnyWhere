use crate::datafusion::SaDataFusion;
use crate::object_storage::{SaS3, SaLocalStorage};
use crate::helper;
use std::sync::Arc;
use datafusion::common::Result;
use datafusion::datasource::file_format::{
    FileFormat,
    csv::CsvFormat,
    parquet::ParquetFormat
};
use datafusion::prelude::DataFrame;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::writer::StreamWriter;
use std::env;


async fn sa_query(sa_datafusion: SaDataFusion, stm: &str) -> Result<DataFrame> {
    let uris: Vec<&str> = helper::sql_parser(stm);
    for uri in uris {
        println!("[sa_query]: Detected URI: {}", uri);
        let file_format: Arc<dyn FileFormat> = if uri.ends_with(".csv") {
            Arc::new(CsvFormat::default())
        } else if uri.ends_with(".parquet") {
            Arc::new(ParquetFormat::default())
        } else {
            panic!("[sa_query]: Unsupported file format")
        };

        if uri.starts_with("file://") {
            let local_storage: SaLocalStorage = SaLocalStorage::new_with_file_uri(uri)
                .init_table_provider(
                    &sa_datafusion,
                    file_format,
                    Some(false)
            ).await?;
            sa_datafusion.register_sa_storage(Arc::new(local_storage)).await?;

        }
        else if uri.starts_with("s3://") {
            let s3_region: String  = env::var("AWS_S3_REGION").unwrap_or("us-east-1".to_string());
            let s3_storage: SaS3 = SaS3::new_from_s3_uri(uri)
                .init_table_provider(
                    s3_region.as_str(),
                    &sa_datafusion,
                    file_format,
                    Some(false)
                ).await?;
                sa_datafusion.register_sa_storage(Arc::new(s3_storage)).await?;
        }
        else {
            panic!("[sa_query]: Unsupported file protocal")
        }
    }
    let df: DataFrame = sa_datafusion.execute_sql(&stm.to_string()).await?;
    Ok(df)
}


pub async fn sa_to_dataframe_pipeline(stm: &str) -> Result<DataFrame> {
    let sa_datafusion: SaDataFusion = SaDataFusion::new();
    Ok(sa_query(sa_datafusion, stm).await?)
}


pub async fn sa_to_arrow_ipc_pipeline(stm: &str) -> Result<Vec<u8>> {
    let sa_datafusion: SaDataFusion = SaDataFusion::new();
    let df: DataFrame = sa_query(sa_datafusion, stm).await?;
    let record_batches: Vec<RecordBatch> = df.collect().await?;
    // Serialize RecordBatches to Arrow IPC
    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut writer: StreamWriter<&mut Vec<u8>> = StreamWriter::try_new(&mut buffer, &record_batches[0].schema())?;
        for batch in record_batches {
            writer.write(&batch)?
        }
        writer.finish()?;
    }

    Ok(buffer)
}