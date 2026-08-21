use graphrecords_core::graphrecord::EdgeDirection;
use pyo3::prelude::*;

#[pyclass(
    frozen,
    eq,
    eq_int,
    hash,
    module = "graphrecords._graphrecords.graphrecord"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PyEdgeDirection {
    Incoming,
    Outgoing,
    Both,
}

impl From<PyEdgeDirection> for EdgeDirection {
    fn from(direction: PyEdgeDirection) -> Self {
        match direction {
            PyEdgeDirection::Incoming => Self::Incoming,
            PyEdgeDirection::Outgoing => Self::Outgoing,
            PyEdgeDirection::Both => Self::Both,
        }
    }
}
