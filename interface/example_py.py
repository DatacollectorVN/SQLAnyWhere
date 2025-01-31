import asyncio

import pandas as pd
import sa_rust
from pyarrow import RecordBatchStreamReader, ipc
from pyarrow.lib import Table


async def run():
    stm = """
    SELECT
        st."id" AS "Student Id",
        st."name" AS "Student Name",
        st."age" AS "Student Age",
        s."subject" AS "Subject",
        s."score" AS "Score"
    FROM
        "s3://sql-anywhere/ex-s3-application/students.csv" AS st
    JOIN
        "file:///Users/nathanngo/Projects/my/SQLAnyWhere/engine/.data/bin/ex-s3-application_v1/scores.csv" AS s
        ON
            st.id = s.student_id
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