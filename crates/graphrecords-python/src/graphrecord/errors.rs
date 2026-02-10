use graphrecords::core::errors::{GraphError, GraphRecordError};
use pyo3::{
    PyErr,
    exceptions::{PyAssertionError, PyIndexError, PyKeyError, PyRuntimeError, PyValueError},
};

#[repr(transparent)]
pub struct PyGraphRecordError(GraphRecordError);

impl From<GraphRecordError> for PyGraphRecordError {
    fn from(error: GraphRecordError) -> Self {
        Self(error)
    }
}

impl From<GraphError> for PyGraphRecordError {
    fn from(error: GraphError) -> Self {
        Self(GraphRecordError::from(error))
    }
}

impl From<PyGraphRecordError> for PyErr {
    fn from(error: PyGraphRecordError) -> Self {
        match error.0 {
            GraphRecordError::IndexError(message) => PyIndexError::new_err(message),
            GraphRecordError::KeyError(message) => PyKeyError::new_err(message),
            GraphRecordError::ConversionError(message) | GraphRecordError::QueryError(message) => {
                PyRuntimeError::new_err(message)
            }
            GraphRecordError::AssertionError(message) => PyAssertionError::new_err(message),
            GraphRecordError::SchemaError(message) => PyValueError::new_err(message),
        }
    }
}
