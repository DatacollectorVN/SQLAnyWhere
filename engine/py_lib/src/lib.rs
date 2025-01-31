use pyo3::prelude::PyResult;
use pyo3::exceptions::PyRuntimeError;
use engine::builder::pipelines;
use pyo3::prelude::*;
use pyo3_asyncio::tokio::future_into_py;


async fn py_sa_to_arrow_ipc_pipeline(stm: &str) -> PyResult<Vec<u8>> {
    Ok(
        pipelines::sa_to_arrow_ipc_pipeline(stm).await.map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to convert PyError: {}", e))
            }
        )?
    )
}


#[pyfunction]
fn execute_sql<'py>(
    py: Python<'py>,
    query: String,
) -> PyResult<&'py PyAny> {
    future_into_py(py, async move { py_sa_to_arrow_ipc_pipeline(query.as_str()).await })
}


/// Python module definition
#[warn(unused_variables)]
#[pymodule]
fn sa_rust(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(execute_sql, m)?)?;
    Ok(())
}