use super::PyGraphRecord;
use graphrecords_core::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{GraphRecord, Writer},
};
use pyo3::{Bound, Py, PyAny, PyErr, Python, prelude::*};
use std::sync::Arc;

#[repr(transparent)]
pub struct PyWriter(Py<PyAny>);

impl PyWriter {
    pub const fn new(writer: Py<PyAny>) -> Self {
        Self(writer)
    }

    fn failure(error: PyErr) -> GraphRecordError {
        GraphRecordError::WriterFailure {
            cause: Arc::new(error),
        }
    }
}

impl Writer for PyWriter {
    type Output = Py<PyAny>;

    fn write(self, graphrecord: &GraphRecord) -> GraphRecordResult<Self::Output> {
        Python::attach(|py| {
            self.0
                .bind(py)
                .call_method1("write", (PyGraphRecord::from(graphrecord.clone()),))
                .map(Bound::unbind)
                .map_err(Self::failure)
        })
    }
}
