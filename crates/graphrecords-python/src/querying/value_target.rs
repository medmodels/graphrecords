use graphrecords_query::dynamic::DynValueTarget;
use pyo3::prelude::*;

#[pyclass(frozen, eq, eq_int)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyValueTarget {
    Value,
    ValueIndex,
    AttributeName,
    AttributeNameIndex,
    NodeIndex,
    EdgeIndex,
    PositionalIndex,
    BoolIndex,
    Mask,
    FailureKind,
    FailureKindIndex,
}

impl From<PyValueTarget> for DynValueTarget {
    fn from(target: PyValueTarget) -> Self {
        match target {
            PyValueTarget::Value => Self::Value,
            PyValueTarget::ValueIndex => Self::ValueIndex,
            PyValueTarget::AttributeName => Self::AttributeName,
            PyValueTarget::AttributeNameIndex => Self::AttributeNameIndex,
            PyValueTarget::NodeIndex => Self::NodeIndex,
            PyValueTarget::EdgeIndex => Self::EdgeIndex,
            PyValueTarget::PositionalIndex => Self::PositionalIndex,
            PyValueTarget::BoolIndex => Self::BoolIndex,
            PyValueTarget::Mask => Self::Mask,
            PyValueTarget::FailureKind => Self::FailureKind,
            PyValueTarget::FailureKindIndex => Self::FailureKindIndex,
        }
    }
}
