pub mod explain;
pub mod manifest;
pub mod operation;
pub mod optimizer;
pub mod prepare;

use crate::resolve_crate_path;
use syn::{Path, Result};

pub fn resolve_core_crate_path() -> Result<Path> {
    resolve_crate_path("graphrecords-core", "core")
}

pub fn resolve_query_crate_path() -> Result<Path> {
    resolve_crate_path("graphrecords-query", "query")
}
