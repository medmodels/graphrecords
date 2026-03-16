mod graph;
mod graphrecord;

pub use graph::GraphError;
pub use graphrecord::GraphRecordError;

impl From<GraphError> for GraphRecordError {
    fn from(value: GraphError) -> Self {
        match value {
            GraphError::IndexError(value) => Self::IndexError(value),
            GraphError::AssertionError(value) => Self::AssertionError(value),
            GraphError::SchemaError(value) => Self::SchemaError(value),
        }
    }
}

pub type GraphRecordResult<T> = Result<T, GraphRecordError>;

#[cfg(test)]
mod test {
    use super::{GraphError, GraphRecordError};

    #[test]
    fn test_from() {
        assert_eq!(
            GraphRecordError::IndexError("value".to_string()),
            GraphRecordError::from(GraphError::IndexError("value".to_string()))
        );
        assert_eq!(
            GraphRecordError::AssertionError("value".to_string()),
            GraphRecordError::from(GraphError::AssertionError("value".to_string()))
        );
        assert_eq!(
            GraphRecordError::SchemaError("value".to_string()),
            GraphRecordError::from(GraphError::SchemaError("value".to_string()))
        );
    }
}
