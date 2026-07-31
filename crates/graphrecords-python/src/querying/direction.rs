use graphrecords_query::EdgeDirection as QueryEdgeDirection;
use pyo3::prelude::*;

#[pyclass(frozen, eq, eq_int)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyEdgeDirection {
    Incoming,
    Outgoing,
    Both,
}

impl From<PyEdgeDirection> for QueryEdgeDirection {
    fn from(direction: PyEdgeDirection) -> Self {
        match direction {
            PyEdgeDirection::Incoming => Self::Incoming,
            PyEdgeDirection::Outgoing => Self::Outgoing,
            PyEdgeDirection::Both => Self::Both,
        }
    }
}
