use graphrecords_query::FailureKind as QueryFailureKind;
use pyo3::prelude::*;

#[pyclass(frozen, eq, hash, module = "graphrecords._graphrecords.querying")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PyFailureKind(QueryFailureKind);

#[pymethods]
impl PyFailureKind {
    #[getter]
    const fn name(&self) -> &'static str {
        self.0.name()
    }

    fn __repr__(&self) -> String {
        format!("FailureKind.{}", self.0.name())
    }
}

impl From<QueryFailureKind> for PyFailureKind {
    fn from(kind: QueryFailureKind) -> Self {
        Self(kind)
    }
}

impl From<PyFailureKind> for QueryFailureKind {
    fn from(kind: PyFailureKind) -> Self {
        kind.0
    }
}
