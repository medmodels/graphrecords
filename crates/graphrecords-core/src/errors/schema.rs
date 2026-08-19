use crate::graphrecord::{AttributeName, EdgeIndex, Group, NodeIndex, datatypes::DataType};
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
    GroupNotInSchema {
        group: Group,
    },
    GroupAlreadyInSchema {
        group: Group,
    },
    NodeAttributeMissing {
        node_index: NodeIndex,
        attribute: AttributeName,
        data_type: DataType,
    },
    EdgeAttributeMissing {
        edge_index: EdgeIndex,
        attribute: AttributeName,
        data_type: DataType,
    },
    NodeAttributeDataTypeMismatch {
        node_index: NodeIndex,
        attribute: AttributeName,
        data_type: DataType,
        expected_data_type: DataType,
    },
    EdgeAttributeDataTypeMismatch {
        edge_index: EdgeIndex,
        attribute: AttributeName,
        data_type: DataType,
        expected_data_type: DataType,
    },
    NodeAttributesNotInSchema {
        node_index: NodeIndex,
        attributes: Vec<AttributeName>,
    },
    EdgeAttributesNotInSchema {
        edge_index: EdgeIndex,
        attributes: Vec<AttributeName>,
    },
    ContinuousAttributeNotNumeric,
    TemporalAttributeNotTemporal,
}

impl Error for SchemaError {}

fn join_attributes(attributes: &[AttributeName]) -> String {
    attributes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

impl Display for SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::GroupNotInSchema { group } => {
                write!(f, "Group `{group}` is not defined in the schema")
            }
            Self::GroupAlreadyInSchema { group } => {
                write!(f, "Group `{group}` already exists in the schema")
            }
            Self::NodeAttributeMissing {
                node_index,
                attribute,
                data_type,
            } => write!(
                f,
                "Attribute `{attribute}` of type `{data_type}` not found on node with index `{node_index}`"
            ),
            Self::EdgeAttributeMissing {
                edge_index,
                attribute,
                data_type,
            } => write!(
                f,
                "Attribute `{attribute}` of type `{data_type}` not found on edge with index `{edge_index}`"
            ),
            Self::NodeAttributeDataTypeMismatch {
                node_index,
                attribute,
                data_type,
                expected_data_type,
            } => write!(
                f,
                "Attribute `{attribute}` of node with index `{node_index}` is of type `{data_type}`. Expected `{expected_data_type}`."
            ),
            Self::EdgeAttributeDataTypeMismatch {
                edge_index,
                attribute,
                data_type,
                expected_data_type,
            } => write!(
                f,
                "Attribute `{attribute}` of edge with index `{edge_index}` is of type `{data_type}`. Expected `{expected_data_type}`."
            ),
            Self::NodeAttributesNotInSchema {
                node_index,
                attributes,
            } => write!(
                f,
                "Attributes [{}] of node with index `{node_index}` do not exist in schema.",
                join_attributes(attributes)
            ),
            Self::EdgeAttributesNotInSchema {
                edge_index,
                attributes,
            } => write!(
                f,
                "Attributes [{}] of edge with index `{edge_index}` do not exist in schema.",
                join_attributes(attributes)
            ),
            Self::ContinuousAttributeNotNumeric => {
                write!(
                    f,
                    "Continuous attribute must be of (sub-)type `Int` or `Float`."
                )
            }
            Self::TemporalAttributeNotTemporal => {
                write!(
                    f,
                    "Temporal attribute must be of (sub-)type `DateTime` or `Duration`."
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::SchemaError;
    use crate::graphrecord::{EdgeIndex, datatypes::DataType};

    #[test]
    fn test_display_groups() {
        assert_eq!(
            "Group `\"test\"` is not defined in the schema",
            SchemaError::GroupNotInSchema {
                group: "test".into()
            }
            .to_string()
        );
        assert_eq!(
            "Group `\"test\"` already exists in the schema",
            SchemaError::GroupAlreadyInSchema {
                group: "test".into()
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_attributes() {
        assert_eq!(
            "Attribute `\"key\"` of type `Int` not found on node with index `\"0\"`",
            SchemaError::NodeAttributeMissing {
                node_index: "0".into(),
                attribute: "key".into(),
                data_type: DataType::Int,
            }
            .to_string()
        );
        assert_eq!(
            "Attribute `\"key\"` of type `Int` not found on edge with index `0000000000000000:0`",
            SchemaError::EdgeAttributeMissing {
                edge_index: EdgeIndex::new(0, 0),
                attribute: "key".into(),
                data_type: DataType::Int,
            }
            .to_string()
        );
        assert_eq!(
            "Attribute `\"key\"` of node with index `\"0\"` is of type `Int`. Expected `Float`.",
            SchemaError::NodeAttributeDataTypeMismatch {
                node_index: "0".into(),
                attribute: "key".into(),
                data_type: DataType::Int,
                expected_data_type: DataType::Float,
            }
            .to_string()
        );
        assert_eq!(
            "Attribute `\"key\"` of edge with index `0000000000000000:0` is of type `Int`. Expected `Float`.",
            SchemaError::EdgeAttributeDataTypeMismatch {
                edge_index: EdgeIndex::new(0, 0),
                attribute: "key".into(),
                data_type: DataType::Int,
                expected_data_type: DataType::Float,
            }
            .to_string()
        );
        assert_eq!(
            "Attributes [\"key1\", \"key2\"] of node with index `\"0\"` do not exist in schema.",
            SchemaError::NodeAttributesNotInSchema {
                node_index: "0".into(),
                attributes: vec!["key1".into(), "key2".into()],
            }
            .to_string()
        );
        assert_eq!(
            "Attributes [\"key1\", \"key2\"] of edge with index `0000000000000000:0` do not exist in schema.",
            SchemaError::EdgeAttributesNotInSchema {
                edge_index: EdgeIndex::new(0, 0),
                attributes: vec!["key1".into(), "key2".into()],
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_attribute_types() {
        assert_eq!(
            "Continuous attribute must be of (sub-)type `Int` or `Float`.",
            SchemaError::ContinuousAttributeNotNumeric.to_string()
        );
        assert_eq!(
            "Temporal attribute must be of (sub-)type `DateTime` or `Duration`.",
            SchemaError::TemporalAttributeNotTemporal.to_string()
        );
    }
}
