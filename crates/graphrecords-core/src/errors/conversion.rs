use crate::graphrecord::Value;
#[cfg(any(feature = "polars", feature = "arrow"))]
use crate::graphrecord::{AttributeName, GroupIndex, datatypes::DataType};
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversionError {
    ValueToIdentifier {
        value: Value,
    },
    #[cfg(any(feature = "polars", feature = "arrow"))]
    UnsupportedFrameValue {
        value: String,
    },
    #[cfg(any(feature = "polars", feature = "arrow"))]
    TimestampOutOfRange {
        timestamp: i64,
    },
    #[cfg(any(feature = "polars", feature = "arrow"))]
    ColumnNotFound {
        column_name: String,
    },
    #[cfg(any(feature = "polars", feature = "arrow"))]
    ReservedAttributeName {
        attribute_name: AttributeName,
    },
    #[cfg(any(feature = "polars", feature = "arrow"))]
    MixedColumnTypes {
        column_name: String,
        data_types: Vec<DataType>,
    },
    #[cfg(any(feature = "polars", feature = "arrow"))]
    NodeDataFrameCreation {
        group_index: Option<GroupIndex>,
    },
    #[cfg(any(feature = "polars", feature = "arrow"))]
    EdgeDataFrameCreation {
        group_index: Option<GroupIndex>,
    },
}

impl Error for ConversionError {}

impl Display for ConversionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::ValueToIdentifier { value } => {
                write!(f, "Cannot convert `{value}` into `Identifier`")
            }
            #[cfg(any(feature = "polars", feature = "arrow"))]
            Self::UnsupportedFrameValue { value } => {
                write!(f, "Cannot convert `{value}` into `Value`")
            }
            #[cfg(any(feature = "polars", feature = "arrow"))]
            Self::TimestampOutOfRange { timestamp } => {
                write!(f, "Cannot convert timestamp `{timestamp}` into a datetime")
            }
            #[cfg(any(feature = "polars", feature = "arrow"))]
            Self::ColumnNotFound { column_name } => {
                write!(
                    f,
                    "Cannot find column with name `{column_name}` in dataframe"
                )
            }
            #[cfg(any(feature = "polars", feature = "arrow"))]
            Self::ReservedAttributeName { attribute_name } => {
                write!(f, "Attribute name `{attribute_name}` is reserved")
            }
            #[cfg(any(feature = "polars", feature = "arrow"))]
            Self::MixedColumnTypes {
                column_name,
                data_types,
            } => {
                let data_types = data_types
                    .iter()
                    .map(|data_type| format!("`{data_type}`"))
                    .collect::<Vec<_>>()
                    .join(", ");

                write!(
                    f,
                    "Cannot build column `{column_name}` from mixed types {data_types}"
                )
            }
            #[cfg(any(feature = "polars", feature = "arrow"))]
            Self::NodeDataFrameCreation { group_index } => {
                let group_index = group_index
                    .as_ref()
                    .map_or_else(|| "ungrouped".to_string(), ToString::to_string);

                write!(
                    f,
                    "Failed to create node DataFrame for group `{group_index}`"
                )
            }
            #[cfg(any(feature = "polars", feature = "arrow"))]
            Self::EdgeDataFrameCreation { group_index } => {
                let group_index = group_index
                    .as_ref()
                    .map_or_else(|| "ungrouped".to_string(), ToString::to_string);

                write!(
                    f,
                    "Failed to create edge DataFrame for group `{group_index}`"
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::ConversionError;
    use crate::graphrecord::Value;
    #[cfg(any(feature = "polars", feature = "arrow"))]
    use crate::graphrecord::datatypes::DataType;

    #[test]
    fn test_display_values() {
        assert_eq!(
            "Cannot convert `true` into `Identifier`",
            ConversionError::ValueToIdentifier {
                value: Value::Bool(true)
            }
            .to_string()
        );
    }

    #[cfg(any(feature = "polars", feature = "arrow"))]
    #[test]
    fn test_display_frame() {
        assert_eq!(
            "Cannot convert `object` into `Value`",
            ConversionError::UnsupportedFrameValue {
                value: "object".to_string()
            }
            .to_string()
        );
        assert_eq!(
            "Cannot convert timestamp `42` into a datetime",
            ConversionError::TimestampOutOfRange { timestamp: 42 }.to_string()
        );
        assert_eq!(
            "Cannot find column with name `index` in dataframe",
            ConversionError::ColumnNotFound {
                column_name: "index".to_string()
            }
            .to_string()
        );
    }

    #[cfg(any(feature = "polars", feature = "arrow"))]
    #[test]
    fn test_display_polars() {
        assert_eq!(
            "Attribute name `\"node_index\"` is reserved",
            ConversionError::ReservedAttributeName {
                attribute_name: "node_index".into()
            }
            .to_string()
        );
        assert_eq!(
            "Cannot build column `count` from mixed types `Int`, `String`",
            ConversionError::MixedColumnTypes {
                column_name: "count".to_string(),
                data_types: vec![DataType::Int, DataType::String]
            }
            .to_string()
        );
        assert_eq!(
            "Failed to create node DataFrame for group `\"dolor\"`",
            ConversionError::NodeDataFrameCreation {
                group_index: Some("dolor".into())
            }
            .to_string()
        );
        assert_eq!(
            "Failed to create node DataFrame for group `ungrouped`",
            ConversionError::NodeDataFrameCreation { group_index: None }.to_string()
        );
        assert_eq!(
            "Failed to create edge DataFrame for group `\"dolor\"`",
            ConversionError::EdgeDataFrameCreation {
                group_index: Some("dolor".into())
            }
            .to_string()
        );
        assert_eq!(
            "Failed to create edge DataFrame for group `ungrouped`",
            ConversionError::EdgeDataFrameCreation { group_index: None }.to_string()
        );
    }
}
