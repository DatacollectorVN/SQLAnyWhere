use datafusion::execution::context::SessionContext;
use datafusion::prelude::{ParquetReadOptions, DataFrame};
use datafusion::error::Result;
use datafusion::arrow::array::RecordBatch;