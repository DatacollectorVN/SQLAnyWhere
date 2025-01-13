
use engine::query::{
    sa_datafusion::SaDataFusion,
    sa_file_options::SaFileOptions
};
use datafusion::prelude::{CsvReadOptions, DataFrame};
use datafusion::arrow::datatypes::Schema;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let sa_datafusion: SaDataFusion = SaDataFusion::new();
    let student_file_path: &str = "src/bin/test_data/example_datafusion_2/students.csv";
    let score_file_path: &str = "src/bin/test_data/example_datafusion_2/scores.csv";
    let sa_csv_default_options: SaFileOptions<'_> = SaFileOptions::Csv(CsvReadOptions::default());
    let student_schema_string: Schema = sa_datafusion.get_cast_schema(student_file_path, &"String", Some(&sa_csv_default_options)).await?;
    let score_schema_string: Schema = sa_datafusion.get_cast_schema(score_file_path, &"String", Some(&sa_csv_default_options)).await?;

    sa_datafusion.register(
        student_file_path,
        Some(
            SaFileOptions::Csv(
                CsvReadOptions {
                    schema: Some(&student_schema_string),
                    ..CsvReadOptions::default()
                }
            )
        )
    ).await?;

    sa_datafusion.register(
        score_file_path,
        Some(
            SaFileOptions::Csv(
                CsvReadOptions {
                    schema: Some(&score_schema_string),
                    ..CsvReadOptions::default()
                }
            )
        )
    ).await?;

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

