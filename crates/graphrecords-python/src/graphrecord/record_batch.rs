use arrow::{
    array::{Array, RecordBatch, RecordBatchIterator, StructArray},
    ffi::to_ffi,
    ffi_stream::FFI_ArrowArrayStream,
};
use pyo3::{
    Bound, PyResult, Python, exceptions::PyRuntimeError, pyclass, pymethods, types::PyCapsule,
};
use std::ffi::CString;

#[pyclass(frozen, module = "graphrecords._graphrecords.graphrecord")]
#[repr(transparent)]
pub struct PyRecordBatch(RecordBatch);

impl From<RecordBatch> for PyRecordBatch {
    fn from(value: RecordBatch) -> Self {
        Self(value)
    }
}

#[pymethods]
impl PyRecordBatch {
    #[pyo3(signature = (requested_schema=None))]
    pub fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
        if requested_schema.is_some() {
            return Err(PyRuntimeError::new_err("Schema casting is not supported"));
        }

        let (array, schema) = to_ffi(&StructArray::from(self.0.clone()).to_data())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

        let schema_capsule = PyCapsule::new(py, schema, Some(CString::new("arrow_schema")?))?;
        let array_capsule = PyCapsule::new(py, array, Some(CString::new("arrow_array")?))?;

        Ok((schema_capsule, array_capsule))
    }

    #[pyo3(signature = (requested_schema=None))]
    pub fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        if requested_schema.is_some() {
            return Err(PyRuntimeError::new_err("Schema casting is not supported"));
        }

        let reader = RecordBatchIterator::new([Ok(self.0.clone())], self.0.schema());
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));

        PyCapsule::new(py, stream, Some(CString::new("arrow_array_stream")?))
    }

    pub fn __len__(&self) -> usize {
        self.0.num_rows()
    }
}
