pub mod explain;
pub mod operation;
pub mod optimizer;

use crate::resolve_crate_path;
use syn::{Path, Result};

pub fn resolve_query_crate_path() -> Result<Path> {
    resolve_crate_path("graphrecords-query", "query")
}
