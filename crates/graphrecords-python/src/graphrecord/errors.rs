use crate::querying::exception::FailureConversion;
use graphrecords_core::errors::{ConversionError, GraphRecordError, IoError, SchemaError};
use graphrecords_overview::OverviewError;
use graphrecords_query::Failure;
use pyo3::{
    PyErr, Python,
    exceptions::{
        PyIndexError, PyKeyError, PyOSError, PyOverflowError, PyRuntimeError, PyValueError,
    },
};
use std::error::Error;

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

impl From<ConversionError> for PyGraphRecordError {
    fn from(error: ConversionError) -> Self {
        Self::GraphRecord(GraphRecordError::from(error))
    }
}

impl From<IoError> for PyGraphRecordError {
    fn from(error: IoError) -> Self {
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

impl PyGraphRecordError {
    fn recover_cause(error: &GraphRecordError) -> Option<PyErr> {
        let mut cause = error.source();

        while let Some(current) = cause {
            if let Some(failure) = current.downcast_ref::<Failure>() {
                return Some(failure.to_python_error());
            }

            if let Some(original) = current.downcast_ref::<PyErr>() {
                return Some(Python::attach(|py| original.clone_ref(py)));
            }

            cause = current.source();
        }

        None
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

        if let Some(original) = PyGraphRecordError::recover_cause(&error) {
            return original;
        }

        let message = error.to_string();

        match error {
            GraphRecordError::NodeNotFound { .. }
            | GraphRecordError::NodesNotFound { .. }
            | GraphRecordError::EdgeNotFound { .. }
            | GraphRecordError::EdgesNotFound { .. }
            | GraphRecordError::GroupNotFound { .. }
            | GraphRecordError::GroupsNotFound { .. } => PyIndexError::new_err(message),
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
            | GraphRecordError::NodeAttributeConflict { .. }
            | GraphRecordError::NoNodeSelected
            | GraphRecordError::NoEdgeSelected
            | GraphRecordError::NoGroupSelected
            | GraphRecordError::IncompatibleValueOperands { .. }
            | GraphRecordError::IncompatibleIdentifierOperands { .. }
            | GraphRecordError::InvalidTimestamp
            | GraphRecordError::Schema(_) => PyValueError::new_err(message),
            GraphRecordError::AddressSpaceExhausted => PyOverflowError::new_err(message),
            GraphRecordError::Io(_) => PyOSError::new_err(message),
            GraphRecordError::PluginFailure { .. }
            | GraphRecordError::WriterFailure { .. }
            | GraphRecordError::QueryFailure { .. }
            | GraphRecordError::Conversion(_) => PyRuntimeError::new_err(message),
        }
    }
}
