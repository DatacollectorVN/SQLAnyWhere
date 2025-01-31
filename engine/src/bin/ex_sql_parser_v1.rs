use engine::helper::sql_parser;
use engine::datafusion::SaDataFusion;
use datafusion::common::Result;
use engine::object_storage::{SaS3, SaLocalStorage};
use datafusion::datasource::file_format::{
    FileFormat,
    csv::CsvFormat,
    parquet::ParquetFormat
};
use datafusion::prelude::DataFrame;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let stm: &str = r#"
    SELECT
        st."id" AS "Student Id",
        st."name" AS "Student Name",
        st."age" AS "Student Age",
        s."subject" AS "Subject",
        s."score" AS "Score"
    FROM
        "s3://sql-anywhere/ex-s3-application/students.csv" AS st
    JOIN
        "file:///Users/nathanngo/Projects/my/SQLAnyWhere/engine/.data/bin/ex-s3-application_v1/scores.csv" AS s
        ON
            st.id = s.student_id
    "#;
    let sa_datafusion: SaDataFusion = SaDataFusion::new();
    let s3_region: &str = "us-east-1";

    let uris: Vec<&str> = sql_parser(stm);
    for uri in uris {
        println!("Detected URI: {}", uri);
        let file_format: Arc<dyn FileFormat> = if uri.ends_with(".csv") {
            Arc::new(CsvFormat::default())
        } else if uri.ends_with(".parquet") {
            Arc::new(ParquetFormat::default())
        } else {
            panic!("Unsupported file format")
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
            let s3_storage: SaS3 = SaS3::new_from_s3_uri(uri)
                .init_table_provider(
                    s3_region,
                    &sa_datafusion,
                    file_format,
                    Some(false)
                ).await?;
                sa_datafusion.register_sa_storage(Arc::new(s3_storage)).await?;
        }
    }
    println!("{}", stm);
    let df: DataFrame = sa_datafusion.execute_sql(&stm.to_string()).await?;
    df.show().await?;
    Ok(())
}