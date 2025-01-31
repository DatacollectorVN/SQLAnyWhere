# 📌 SQLAnyWhere - Query anywhere from your local!

🚀 SQLAnyWhere is a high-performance, cloud-native SQL query engine designed to process structured data efficiently and at scale. Built with Rust, it offers seamless cloud integration, enabling direct SQL queries on data stored in Amazon S3, local Parquet, and other storage backends.

## 🔹 Features
✅ Cloud-Native SQL Engine - Query data directly from Amazon S3, Google Cloud Storage (GCS), and Azure Blob Storage.

*Note:* Now it only support for S3.

✅ Blazing-Fast Performance - Optimized columnar execution using Apache Arrow & DataFusion.

✅ Flexible Data Storage - Supports Parquet, CSV, JSON, and Iceberg tables.

✅ Python & Rust Integration - Expose query results via Arrow IPC for zero-copy transfer to Pandas/Spark.

✅ Distributed Execution Ready - Designed for parallel execution in distributed environments.

✅ Pluggable Storage & Compute - Extendable to support custom data sources & compute backends.


## How to use
### With Python
Make sure you already install [rust](https://www.rust-lang.org/).

Create conda env with Python3.12:
```bash
conda create -n sql_anywhere python=3.12 -y

conda activate sql_anywhere
```

Install dependencies:
```bash
pip install -r interface/requirements.txt
```


Query your file:
```python
async def run():
    stm = """
        SELECT
                *
        FROM
            "<file_protocal>://<absolute_file_path>"
    """
    binary_data: list = await sa_rust.execute_sql(stm)
    binary_bytes: bytes = bytes(binary_data)
    reader: RecordBatchStreamReader = ipc.open_stream(binary_bytes)

    # Read all Arrow data (without unnecessary deserialization)
    table: Table = reader.read_all()

    # Convert to Pandas DataFrame
    df: pd.DataFrame = table.to_pandas()
    print(df)

asyncio.run(run())
```

File protocal:
- Local file: `file`. I.e: file://<absolute_file_path>.
- S3: `s3`. I.e: s3://<s3_source_key>/<s3_file>.

Please read `interface/example_py.py` for more understanding.