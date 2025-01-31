use engine::builder::pipelines::sa_to_arrow_ipc_pipeline;
use datafusion::common::Result;

#[tokio::main]
async fn main() -> Result<()>{
    let stm: &str = r#"SELECT
            st."id" AS "Student Id",
            st."name" AS "Student Name",
            st."age" AS "Student Age",
            s."subject" AS "Subject",
            s."score" AS "Score"
        FROM
            "file:///Users/nathanngo/Projects/my/SQLAnyWhere/engine/.data/bin/ex-local-storage-application/students.csv" AS st
        JOIN
            "file:///Users/nathanngo/Projects/my/SQLAnyWhere/engine/.data/bin/ex-local-storage-application/scores.csv" AS s
            ON
                st.id = s.student_id
    "#;
    let buffer: Vec<u8> = sa_to_arrow_ipc_pipeline(stm).await?;
    println!("{:?}", buffer);
    Ok(())
}