use engine::object_storage::storage::SaStorage;
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
    sa_datafusion.register_sa_storage(Arc::new(score_storage.clone())).await?;
    println!("Score schema:");
    sa_datafusion.display_schema(&score_storage.get_file_url().as_str()).await?;
    println!();

    println!("Initializing and registering student.csv into SQLAnyWhere...");
    let student_storage: SaLocalStorage = SaLocalStorage::new(&student_path)
        .init_table_provider(
            &sa_datafusion,
            Arc::new(CsvFormat::default()),
            Some(false)
        ).await?;
    sa_datafusion.register_sa_storage(Arc::new(student_storage.clone())).await?;
    println!("Student schema:");
    sa_datafusion.display_schema(&student_storage.get_file_url().as_str()).await?;
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
    let df: DataFrame = sa_datafusion.execute_sql(&stm.as_str()).await?;
    df.show().await?;
    Ok(())
}