use std::fs::File;

use engine::modules::datafusion::{SADataFusion, FileFormatOptions};
use datafusion::prelude::{CsvReadOptions, DataFrame};
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::Schema;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sa_datafusion: SADataFusion = SADataFusion::new();
    let student_file_path: &str = "src/bin/test_data/example_datafusion_2/students.csv";
    let score_file_path: &str = "src/bin/test_data/example_datafusion_2/scores.csv";

    let student_schema_string: Schema = sa_datafusion.cast_all_columns(student_file_path, &"String").await?;
    let score_schema_string: Schema = sa_datafusion.cast_all_columns(score_file_path, &"String").await?;

    sa_datafusion.register_file(
        student_file_path,
        Some(
            FileFormatOptions::Csv(
                CsvReadOptions {
                    schema: Some(&student_schema_string),
                    ..CsvReadOptions::default()
                }
            )
        )
    ).await?;

    sa_datafusion.register_file(
        score_file_path,
        Some(
            FileFormatOptions::Csv(
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

