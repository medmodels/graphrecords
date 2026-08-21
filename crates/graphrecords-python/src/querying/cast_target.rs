use graphrecords_query::dynamic::DynCastTarget;
use pyo3::prelude::*;

#[pyclass(
    frozen,
    eq,
    eq_int,
    hash,
    module = "graphrecords._graphrecords.querying"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PyCastTarget {
    Bool,
    DateTime,
    Duration,
    Float,
    Int,
    String,
}

impl From<PyCastTarget> for DynCastTarget {
    fn from(target: PyCastTarget) -> Self {
        match target {
            PyCastTarget::Bool => Self::Bool,
            PyCastTarget::DateTime => Self::DateTime,
            PyCastTarget::Duration => Self::Duration,
            PyCastTarget::Float => Self::Float,
            PyCastTarget::Int => Self::Int,
            PyCastTarget::String => Self::String,
        }
    }
}
