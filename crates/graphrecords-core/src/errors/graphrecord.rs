use std::{
    error::Error,
    fmt::{Display, Formatter, Result},
};

#[derive(Debug, PartialEq)]
pub enum GraphRecordError {
    IndexError(String),
    KeyError(String),
    ConversionError(String),
    AssertionError(String),
    SchemaError(String),
    QueryError(String),
}

impl Error for GraphRecordError {
    fn description(&self) -> &str {
        match self {
            GraphRecordError::IndexError(message) => message,
            GraphRecordError::KeyError(message) => message,
            GraphRecordError::ConversionError(message) => message,
            GraphRecordError::AssertionError(message) => message,
            GraphRecordError::SchemaError(message) => message,
            GraphRecordError::QueryError(message) => message,
        }
    }
}

impl Display for GraphRecordError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            Self::IndexError(message) => write!(f, "IndexError: {message}"),
            Self::KeyError(message) => write!(f, "KeyError: {message}"),
            Self::ConversionError(message) => write!(f, "ConversionError: {message}"),
            Self::AssertionError(message) => write!(f, "AssertionError: {message}"),
            Self::SchemaError(message) => write!(f, "SchemaError: {message}"),
            Self::QueryError(message) => write!(f, "QueryError: {message}"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::GraphRecordError;

    #[test]
    fn test_display() {
        assert_eq!(
            "IndexError: value",
            GraphRecordError::IndexError("value".to_string()).to_string()
        );
        assert_eq!(
            "KeyError: value",
            GraphRecordError::KeyError("value".to_string()).to_string()
        );
        assert_eq!(
            "ConversionError: value",
            GraphRecordError::ConversionError("value".to_string()).to_string()
        );
        assert_eq!(
            "AssertionError: value",
            GraphRecordError::AssertionError("value".to_string()).to_string()
        );
        assert_eq!(
            "SchemaError: value",
            GraphRecordError::SchemaError("value".to_string()).to_string()
        );
    }
}
