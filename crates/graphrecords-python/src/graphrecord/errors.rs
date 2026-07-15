use graphrecords_core::errors::{GraphRecordError, SchemaError};
use pyo3::{
    PyErr,
    exceptions::{PyAssertionError, PyIndexError, PyKeyError, PyRuntimeError, PyValueError},
};

pub enum PyGraphRecordError {
    GraphRecord(GraphRecordError),
    Conversion(String),
}
pub type PyGraphRecordResult<T> = Result<T, PyGraphRecordError>;

impl From<GraphRecordError> for PyGraphRecordError {
    fn from(error: GraphRecordError) -> Self {
        Self::GraphRecord(error)
    }
}

impl From<SchemaError> for PyGraphRecordError {
    fn from(error: SchemaError) -> Self {
        Self::GraphRecord(GraphRecordError::from(error))
    }
}

impl From<PyGraphRecordError> for PyErr {
    fn from(error: PyGraphRecordError) -> Self {
        let error = match error {
            PyGraphRecordError::GraphRecord(error) => error,
            PyGraphRecordError::Conversion(message) => {
                return PyRuntimeError::new_err(message);
            }
        };

        let message = error.to_string();

        match error {
            GraphRecordError::NodeNotFound { .. }
            | GraphRecordError::EdgeNotFound { .. }
            | GraphRecordError::GroupNotFound { .. } => PyIndexError::new_err(message),
            GraphRecordError::NodeAttributeNotFound { .. }
            | GraphRecordError::EdgeAttributeNotFound { .. }
            | GraphRecordError::PluginNotFound { .. }
            | GraphRecordError::PluginAlreadyExists { .. } => PyKeyError::new_err(message),
            GraphRecordError::NodeAlreadyExists { .. }
            | GraphRecordError::GroupAlreadyExists { .. }
            | GraphRecordError::NodeAlreadyInGroup { .. }
            | GraphRecordError::EdgeAlreadyInGroup { .. }
            | GraphRecordError::NodeNotInGroup { .. }
            | GraphRecordError::EdgeNotInGroup { .. }
            | GraphRecordError::IncompatibleValueOperands { .. }
            | GraphRecordError::IncompatibleAttributeOperands { .. }
            | GraphRecordError::InvalidTimestamp => PyAssertionError::new_err(message),
            GraphRecordError::Schema(_) => PyValueError::new_err(message),
            GraphRecordError::PluginFailure { .. }
            | GraphRecordError::ConnectorFailure { .. }
            | GraphRecordError::Conversion(_)
            | GraphRecordError::QueryError(_) => PyRuntimeError::new_err(message),
        }
    }
}
