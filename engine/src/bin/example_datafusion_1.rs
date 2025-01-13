use engine::sa_datafusion::{
    SaDataFusion,
    sa_file_options::SaFileOptions
};
use datafusion::prelude::{ParquetReadOptions, DataFrame};
use datafusion::common::DFSchema;
use datafusion::arrow::datatypes::Schema;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sa_datafusion: SaDataFusion = SaDataFusion::new();
    let parquet_path: &str = "src/bin/test_data/example_datafusion_1/house_price.parquet";
    let string_schema: Schema = sa_datafusion.get_cast_schema(parquet_path, &"String", Some(&SaFileOptions::Parquet(ParquetReadOptions::default()))).await?;
    let read_options: ParquetReadOptions<'_> = ParquetReadOptions {
        schema: Some(&string_schema),
        ..ParquetReadOptions::default()
    };
    sa_datafusion.register(parquet_path, Some(SaFileOptions::Parquet(read_options))).await?;

    let df: DataFrame = sa_datafusion.execute_sql(r#"SELECT * FROM "house_price""#).await?;
    let schema: DFSchema = df.schema().clone();
    df.limit(0, Some(10))?.show().await?;

    println!("Schema:");
    for field in schema.fields() {
        println!("Column: {}, Data Type: {:?}", field.name(), field.data_type());
    }

    Ok(())
}