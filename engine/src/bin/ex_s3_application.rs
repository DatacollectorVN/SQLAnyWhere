use engine::object_storage::storage::SaStorage;
use engine::object_storage::{SaS3, SaLocalStorage};
use datafusion::common::Result;
use engine::datafusion::SaDataFusion;
use std::sync::Arc;
use datafusion::prelude::DataFrame;
use datafusion::datasource::file_format::csv::CsvFormat;


#[tokio::main]
async fn main() -> Result<()> {
    let s3_bucket: &str = "sql-anywhere";
    let s3_src_key: &str = "ex-s3-application";
    let s3_file: &str = "students.csv";
    let s3_region: &str = "us-east-1";

    let sa_datafusion: SaDataFusion = SaDataFusion::new();

    let student_storage: SaS3 = SaS3::new(s3_bucket, s3_src_key, s3_file)
        .init_table_provider(
            s3_region,
            &sa_datafusion,
            Arc::new(CsvFormat::default()),
            Some(false)
        ).await?;
    sa_datafusion.register_sa_storage(Arc::new(student_storage.clone())).await?;
    println!("Students schema:");
    sa_datafusion.display_schema(student_storage.get_file_url().as_str()).await?;
    println!();

    let base_path: &str = env!("CARGO_MANIFEST_DIR");
    let score_path: String = format!("{}/{}", base_path, ".data/bin/ex-s3-application/scores.csv");
    let score_storage: SaLocalStorage = SaLocalStorage::new(&score_path)
        .init_table_provider(
            &sa_datafusion,
            Arc::new(CsvFormat::default()),
            Some(false)
        ).await?;
    sa_datafusion.register_sa_storage(Arc::new(score_storage.clone())).await?;
    println!("Score schema:");
    sa_datafusion.display_schema(score_storage.get_file_url().as_str()).await?;
    println!();

    println!("Joining and showing scores and students...");
    let stm: String = format!(r#"
        SELECT
            st."id" AS "Student Id",
            st."name" AS "Student Name",
            st."age" AS "Student Age",
            s."subject" AS "Subject",
            s."score" AS "Score"
        FROM
            "{}" AS st
        JOIN
            "{}" AS s
            ON
                st.id = s.student_id
        "#, student_storage.get_file_url(), score_storage.get_file_url());
    println!("{}", stm);
    let df: DataFrame = sa_datafusion.execute_sql(&stm.to_string()).await?;
    df.show().await?;
    Ok(())
}
