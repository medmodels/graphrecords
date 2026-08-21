use super::{errors::PyGraphRecordError, traits::DeepFrom};
use graphrecords_core::graphrecord::EdgeIndex;
use pyo3::{
    prelude::*,
    types::{PyBytes, PyBytesMethods, PyTuple},
};
use std::ops::Deref;

#[pyclass(frozen, eq, hash, module = "graphrecords._graphrecords.graphrecord")]
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PyEdgeIndex(EdgeIndex);

impl From<EdgeIndex> for PyEdgeIndex {
    fn from(value: EdgeIndex) -> Self {
        Self(value)
    }
}

impl From<PyEdgeIndex> for EdgeIndex {
    fn from(value: PyEdgeIndex) -> Self {
        value.0
    }
}

impl DeepFrom<EdgeIndex> for PyEdgeIndex {
    fn deep_from(value: EdgeIndex) -> Self {
        value.into()
    }
}

impl DeepFrom<PyEdgeIndex> for EdgeIndex {
    fn deep_from(value: PyEdgeIndex) -> Self {
        value.into()
    }
}

impl Deref for PyEdgeIndex {
    type Target = EdgeIndex;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl PyEdgeIndex {
    #[staticmethod]
    pub fn _from_bytes(data: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let edge_index: EdgeIndex = bincode::deserialize(data.as_bytes()).map_err(|_| {
            PyGraphRecordError::Conversion("Failed to deserialize EdgeIndex".to_string())
        })?;

        Ok(Self(edge_index))
    }

    pub fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Py<PyAny>, Bound<'py, PyTuple>)> {
        let bytes = bincode::serialize(&self.0).map_err(|_| {
            PyGraphRecordError::Conversion("Failed to serialize EdgeIndex".to_string())
        })?;
        let constructor = py.get_type::<Self>().getattr("_from_bytes")?.unbind();
        let arguments = (PyBytes::new(py, &bytes),).into_pyobject(py)?;

        Ok((constructor, arguments))
    }

    pub fn __repr__(&self) -> String {
        format!("EdgeIndex({})", self.0)
    }

    pub fn __str__(&self) -> String {
        self.0.to_string()
    }
}
