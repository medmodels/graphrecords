use graphrecords_query::index::EdgeEndpointRole as QueryEdgeEndpointRole;
use pyo3::prelude::*;

#[pyclass(frozen, eq, eq_int, hash)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PyEdgeEndpointRole {
    Source,
    Target,
}

impl From<QueryEdgeEndpointRole> for PyEdgeEndpointRole {
    fn from(role: QueryEdgeEndpointRole) -> Self {
        match role {
            QueryEdgeEndpointRole::Source => Self::Source,
            QueryEdgeEndpointRole::Target => Self::Target,
        }
    }
}
