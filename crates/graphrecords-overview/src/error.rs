use graphrecords_core::errors::GraphRecordError;
use graphrecords_query::Failure;
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

pub type OverviewResult<T> = Result<T, OverviewError>;

#[derive(Debug)]
pub enum OverviewError {
    GraphRecord(GraphRecordError),
    Query(Box<Failure>),
}

impl Error for OverviewError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::GraphRecord(error) => Some(error),
            Self::Query(failure) => Some(failure),
        }
    }
}

impl From<GraphRecordError> for OverviewError {
    fn from(error: GraphRecordError) -> Self {
        Self::GraphRecord(error)
    }
}

impl From<Box<Failure>> for OverviewError {
    fn from(failure: Box<Failure>) -> Self {
        Self::Query(failure)
    }
}

impl Display for OverviewError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::GraphRecord(error) => write!(f, "{error}"),
            Self::Query(failure) => write!(f, "{failure}"),
        }
    }
}
