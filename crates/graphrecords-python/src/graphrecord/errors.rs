use crate::querying::exception::FailureConversion;
use graphrecords_core::errors::{GraphRecordError, SchemaError};
use graphrecords_overview::OverviewError;
use graphrecords_query::Failure;
use pyo3::{
    PyErr,
    exceptions::{PyAssertionError, PyIndexError, PyKeyError, PyRuntimeError, PyValueError},
};

pub enum PyGraphRecordError {
    GraphRecord(GraphRecordError),
    Conversion(String),
    Query(Box<Failure>),
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

impl From<OverviewError> for PyGraphRecordError {
    fn from(error: OverviewError) -> Self {
        match error {
            OverviewError::GraphRecord(error) => Self::GraphRecord(error),
            OverviewError::Query(failure) => Self::Query(failure),
        }
    }
}

impl From<PyGraphRecordError> for PyErr {
    fn from(error: PyGraphRecordError) -> Self {
        let error = match error {
            PyGraphRecordError::GraphRecord(error) => error,
            PyGraphRecordError::Conversion(message) => {
                return PyRuntimeError::new_err(message);
            }
            PyGraphRecordError::Query(failure) => {
                return failure.to_python_error();
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
            | GraphRecordError::IncompatibleIdentifierOperands { .. }
            | GraphRecordError::InvalidTimestamp => PyAssertionError::new_err(message),
            GraphRecordError::Schema(_) => PyValueError::new_err(message),
            GraphRecordError::PluginFailure { .. }
            | GraphRecordError::ConnectorFailure { .. }
            | GraphRecordError::Conversion(_) => PyRuntimeError::new_err(message),
        }
    }
}
