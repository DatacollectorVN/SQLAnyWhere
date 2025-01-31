use engine::object_storage::storage::SaStorage;
use engine::object_storage::SaS3;
use datafusion::common::Result;
use engine::datafusion::SaDataFusion;
use std::sync::Arc;
use datafusion::prelude::DataFrame;
use datafusion::datasource::file_format::parquet::ParquetFormat;


#[tokio::main]
async fn main() -> Result<()> {
    let s3_uri: &str = "s3://sql-anywhere/ex-s3-application/house-price.parquet";
    let s3_region: &str = "us-east-1";

    let sa_datafusion: SaDataFusion = SaDataFusion::new();
    let house_price_storage: SaS3 = SaS3::new_from_s3_uri(s3_uri)
        .init_table_provider(
            s3_region,
            &sa_datafusion,
            Arc::new(ParquetFormat::default()),
            Some(false)
        ).await?;

    sa_datafusion.register_sa_storage(Arc::new(house_price_storage.clone())).await?;
    println!("{}", house_price_storage.get_s3_bucket());
    println!("{}", house_price_storage.get_s3_src_key());
    println!("{}", house_price_storage.get_s3_file());

    let stm: String = format!(r#"
        SELECT
            *
        FROM
            "{}"
        "#, house_price_storage.get_file_url());
    println!("{}", stm);
    let df: DataFrame = sa_datafusion.execute_sql(&stm.to_string()).await?;
    df.show().await?;
    Ok(())
}
