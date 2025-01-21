use engine::object_storage::SaLocalStorage;
use datafusion::common::Result;
use engine::datafusion::SaDataFusion;
use std::sync::Arc;
use datafusion::prelude::DataFrame;
use datafusion::datasource::file_format::csv::CsvFormat;


#[tokio::main]
async fn main() -> Result<()> {
    let base_path: &str = env!("CARGO_MANIFEST_DIR");
    let score_path: String = format!("{}/{}", base_path, ".data/bin/ex-local-storage-application/scores.csv");
    let student_path: String = format!("{}/{}", base_path, ".data/bin/ex-local-storage-application/students.csv");
    let sa_datafusion: SaDataFusion = SaDataFusion::new();

    println!("Initializing and registering scores.csv into SQLAnyWhere...");
    let score_storage: SaLocalStorage = SaLocalStorage::new(&score_path)
        .init_table_provider(
            &sa_datafusion,
            Arc::new(CsvFormat::default()),
            Some(false)
        ).await?;
    sa_datafusion.register_storage(Arc::new(score_storage)).await?;
    println!("Score schema:");
    sa_datafusion.display_schema("scores").await?;
    println!();

    println!("Initializing and registering student.csv into SQLAnyWhere...");
    let student_storage = SaLocalStorage::new(&student_path)
        .init_table_provider(
            &sa_datafusion,
            Arc::new(CsvFormat::default()),
            Some(false)
        ).await?;
    sa_datafusion.register_storage(Arc::new(student_storage)).await?;
    println!("Score schema:");
    sa_datafusion.display_schema("scores").await?;
    println!();

    println!("Joining and showing scores and students...");
    let stm: &str = "
        SELECT
            *
        FROM
            students AS st
        JOIN
            scores AS s
            ON
                st.id = s.student_id
    ";
    let df: DataFrame = sa_datafusion.execute_sql(stm).await?;
    df.show().await?;
    Ok(())
}