use engine::object_storage::local_storage::SaLocalStorage;
use datafusion::common::Result;
use engine::datafusion::SaDataFusion;
use datafusion::execution::SessionState;
use std::sync::Arc;
use datafusion::prelude::DataFrame;
use datafusion::datasource::file_format::csv::CsvFormat;


#[tokio::main]
async fn main() -> Result<()> {
    let base_path: &str = env!("CARGO_MANIFEST_DIR");
    let score_path: String = format!("{}/{}", base_path, "src/bin/test_data/example_local_storage_application/scores.csv");
    let student_path: String = format!("{}/{}", base_path, "src/bin/test_data/example_local_storage_application/students.csv");;
    let sa_datafusion: SaDataFusion = SaDataFusion::new();
    let session_state: SessionState = sa_datafusion.get_session_state();

    println!("Initializing and registering scores.csv into SQLAnyWhere...");
    let score_storage: SaLocalStorage = SaLocalStorage::new(score_path.as_str(), &session_state, Arc::new(CsvFormat::default())).await?;
    sa_datafusion.register_storage(Arc::new(score_storage)).await?;

    println!("Initializing and registering student.csv into SQLAnyWhere...");
    let student_storage: SaLocalStorage = SaLocalStorage::new(student_path.as_str(), &session_state, Arc::new(CsvFormat::default())).await?;
    sa_datafusion.register_storage(Arc::new(student_storage)).await?;

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