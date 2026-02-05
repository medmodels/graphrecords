use graphrecords::core::GraphRecord;
use pyo3::{
    types::{PyAnyMethods, PyBytes, PyBytesMethods},
    Bound, FromPyObject, IntoPyObject, PyAny, PyErr, PyResult, Python,
};

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PyGraphRecord(pub GraphRecord);

impl From<GraphRecord> for PyGraphRecord {
    fn from(value: GraphRecord) -> Self {
        PyGraphRecord(value)
    }
}

impl From<PyGraphRecord> for GraphRecord {
    fn from(value: PyGraphRecord) -> Self {
        value.0
    }
}

impl AsRef<GraphRecord> for PyGraphRecord {
    fn as_ref(&self) -> &GraphRecord {
        &self.0
    }
}

impl<'py> FromPyObject<'py> for PyGraphRecord {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let bytes = ob.call_method0("_to_bytes")?;
        let py_bytes: &Bound<'py, PyBytes> = bytes.downcast()?;

        let graphrecord = bincode::deserialize(py_bytes.as_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        Ok(Self(graphrecord))
    }
}

impl<'py> IntoPyObject<'py> for PyGraphRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let bytes = bincode::serialize(&self.0)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let py_bytes = PyBytes::new(py, &bytes);

        let py_graphrecord_class = py
            .import("graphrecords._graphrecords")?
            .getattr("PyGraphRecord")?;
        let obj = py_graphrecord_class.call_method1("_from_bytes", (py_bytes,))?;

        Ok(obj)
    }
}
