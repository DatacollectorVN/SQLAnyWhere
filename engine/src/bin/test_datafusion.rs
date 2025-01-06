use engine::modules::datafusion::{SADataFusion, FileFormatOptions};
use datafusion::prelude::{ParquetReadOptions, DataFrame};
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::Schema;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sa_datafusion: SADataFusion = SADataFusion::new();
    let parquet_path: &str = "src/data/test_datafusion/house_price.parquet";
    let string_schema: Schema = sa_datafusion.cast_all_columns(parquet_path, &"String").await?;
    let read_options: ParquetReadOptions<'_> = ParquetReadOptions {
        schema: Some(&string_schema),
        ..ParquetReadOptions::default()
    };
    sa_datafusion.register_file(parquet_path, Some(FileFormatOptions::Parquet(read_options))).await?;

    let df: DataFrame = sa_datafusion.execute_sql(r#"SELECT * FROM "house_price""#).await?;
    let schema:DFSchema = df.schema().clone();
    df.limit(0, Some(10))?.show().await?;

    println!("Schema:");
    for field in schema.fields() {
        println!("Column: {}, Data Type: {:?}", field.name(), field.data_type());
    }

    Ok(())
}