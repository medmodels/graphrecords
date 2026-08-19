use crate::graphrecord::{AttributeName, Value};
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    io::ErrorKind,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversionError {
    ValueToIdentifier { value: Value },
    UnsupportedPolarsValue { value: String },
    UnsupportedPolarsIdentifier { value: String },
    TimestampOutOfRange { timestamp: i64 },
    ColumnNotFound { column_name: String },
    ReservedAttributeName { attribute: AttributeName },
    NodeDataFrameCreation { group: String },
    EdgeDataFrameCreation { group: String },
    FileRead { path: String, kind: ErrorKind },
    FileWrite { path: String, kind: ErrorKind },
    DirectoryCreation { path: String, kind: ErrorKind },
    RonSerialization,
    RonDeserialization { path: String },
    BinarySerialization,
    BinaryDeserialization,
}

impl Error for ConversionError {}

impl Display for ConversionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::ValueToIdentifier { value } => {
                write!(f, "Cannot convert `{value}` into `Identifier`")
            }
            Self::UnsupportedPolarsValue { value } => {
                write!(f, "Cannot convert `{value}` into `Value`")
            }
            Self::UnsupportedPolarsIdentifier { value } => {
                write!(f, "Cannot convert `{value}` into `Identifier`")
            }
            Self::TimestampOutOfRange { timestamp } => {
                write!(f, "Cannot convert timestamp `{timestamp}` into a datetime")
            }
            Self::ColumnNotFound { column_name } => {
                write!(
                    f,
                    "Cannot find column with name `{column_name}` in dataframe"
                )
            }
            Self::ReservedAttributeName { attribute } => {
                write!(f, "Attribute name `{attribute}` is reserved")
            }
            Self::NodeDataFrameCreation { group } => {
                write!(f, "Failed to create node DataFrame for group `{group}`")
            }
            Self::EdgeDataFrameCreation { group } => {
                write!(f, "Failed to create edge DataFrame for group `{group}`")
            }
            Self::FileRead { path, kind } => {
                write!(f, "Failed to read file `{path}`: {kind}")
            }
            Self::FileWrite { path, kind } => {
                write!(f, "Failed to write file `{path}`: {kind}")
            }
            Self::DirectoryCreation { path, kind } => {
                write!(f, "Failed to create directory `{path}`: {kind}")
            }
            Self::RonSerialization => write!(f, "Failed to convert GraphRecord to ron"),
            Self::RonDeserialization { path } => {
                write!(f, "Failed to create GraphRecord from file `{path}`")
            }
            Self::BinarySerialization => write!(f, "Could not serialize GraphRecord"),
            Self::BinaryDeserialization => write!(f, "Could not deserialize GraphRecord"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::ConversionError;
    use crate::graphrecord::Value;
    use std::io::ErrorKind;

    #[test]
    fn test_display_values() {
        assert_eq!(
            "Cannot convert `true` into `Identifier`",
            ConversionError::ValueToIdentifier {
                value: Value::Bool(true)
            }
            .to_string()
        );
        assert_eq!(
            "Cannot convert `true` into `Value`",
            ConversionError::UnsupportedPolarsValue {
                value: "true".to_string()
            }
            .to_string()
        );
        assert_eq!(
            "Cannot convert `true` into `Identifier`",
            ConversionError::UnsupportedPolarsIdentifier {
                value: "true".to_string()
            }
            .to_string()
        );
        assert_eq!(
            "Cannot convert timestamp `1` into a datetime",
            ConversionError::TimestampOutOfRange { timestamp: 1 }.to_string()
        );
    }

    #[test]
    fn test_display_dataframes() {
        assert_eq!(
            "Cannot find column with name `index` in dataframe",
            ConversionError::ColumnNotFound {
                column_name: "index".to_string()
            }
            .to_string()
        );
        assert_eq!(
            "Attribute name `\"node_index\"` is reserved",
            ConversionError::ReservedAttributeName {
                attribute: "node_index".into()
            }
            .to_string()
        );
        assert_eq!(
            "Failed to create node DataFrame for group `group`",
            ConversionError::NodeDataFrameCreation {
                group: "group".to_string()
            }
            .to_string()
        );
        assert_eq!(
            "Failed to create edge DataFrame for group `group`",
            ConversionError::EdgeDataFrameCreation {
                group: "group".to_string()
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_files() {
        assert_eq!(
            "Failed to read file `path`: entity not found",
            ConversionError::FileRead {
                path: "path".to_string(),
                kind: ErrorKind::NotFound
            }
            .to_string()
        );
        assert_eq!(
            "Failed to write file `path`: permission denied",
            ConversionError::FileWrite {
                path: "path".to_string(),
                kind: ErrorKind::PermissionDenied
            }
            .to_string()
        );
        assert_eq!(
            "Failed to create directory `path`: permission denied",
            ConversionError::DirectoryCreation {
                path: "path".to_string(),
                kind: ErrorKind::PermissionDenied
            }
            .to_string()
        );
        assert_eq!(
            "Failed to convert GraphRecord to ron",
            ConversionError::RonSerialization.to_string()
        );
        assert_eq!(
            "Failed to create GraphRecord from file `path`",
            ConversionError::RonDeserialization {
                path: "path".to_string()
            }
            .to_string()
        );
        assert_eq!(
            "Could not serialize GraphRecord",
            ConversionError::BinarySerialization.to_string()
        );
        assert_eq!(
            "Could not deserialize GraphRecord",
            ConversionError::BinaryDeserialization.to_string()
        );
    }
}
