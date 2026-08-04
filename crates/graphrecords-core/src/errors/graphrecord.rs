use super::{conversion::ConversionError, schema::SchemaError};
use crate::graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, Group, NodeIndex};
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueOperation {
    Add,
    Subtract,
    Multiply,
    Divide,
    Power,
    Modulo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphRecordError {
    NodeNotFound {
        node_index: NodeIndex,
    },
    EdgeNotFound {
        edge_index: EdgeIndex,
    },
    NodeAlreadyExists {
        node_index: NodeIndex,
    },
    GroupNotFound {
        group: Group,
    },
    GroupAlreadyExists {
        group: Group,
    },
    NodeAlreadyInGroup {
        node_index: NodeIndex,
        group: Group,
    },
    EdgeAlreadyInGroup {
        edge_index: EdgeIndex,
        group: Group,
    },
    NodeNotInGroup {
        node_index: NodeIndex,
        group: Group,
    },
    EdgeNotInGroup {
        edge_index: EdgeIndex,
        group: Group,
    },
    NodeAttributeNotFound {
        node_index: NodeIndex,
        attribute: GraphRecordAttribute,
    },
    EdgeAttributeNotFound {
        edge_index: EdgeIndex,
        attribute: GraphRecordAttribute,
    },
    IncompatibleValueOperands {
        operation: ValueOperation,
        left: GraphRecordValue,
        right: GraphRecordValue,
    },
    IncompatibleAttributeOperands {
        operation: ValueOperation,
        left: GraphRecordAttribute,
        right: GraphRecordAttribute,
    },
    InvalidTimestamp,
    PluginNotFound {
        name: GraphRecordAttribute,
    },
    PluginAlreadyExists {
        name: GraphRecordAttribute,
    },
    PluginFailure {
        message: String,
    },
    ConnectorFailure {
        message: String,
    },
    Schema(SchemaError),
    Conversion(ConversionError),
}

impl Error for GraphRecordError {}

impl From<SchemaError> for GraphRecordError {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<ConversionError> for GraphRecordError {
    fn from(error: ConversionError) -> Self {
        Self::Conversion(error)
    }
}

impl Display for GraphRecordError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NodeNotFound { node_index } => {
                write!(f, "Cannot find node with index `{node_index}`")
            }
            Self::EdgeNotFound { edge_index } => {
                write!(f, "Cannot find edge with index `{edge_index}`")
            }
            Self::NodeAlreadyExists { node_index } => {
                write!(f, "Node with index `{node_index}` already exists")
            }
            Self::GroupNotFound { group } => write!(f, "Cannot find group `{group}`"),
            Self::GroupAlreadyExists { group } => write!(f, "Group `{group}` already exists"),
            Self::NodeAlreadyInGroup { node_index, group } => {
                write!(
                    f,
                    "Node with index `{node_index}` already in group `{group}`"
                )
            }
            Self::EdgeAlreadyInGroup { edge_index, group } => {
                write!(
                    f,
                    "Edge with index `{edge_index}` already in group `{group}`"
                )
            }
            Self::NodeNotInGroup { node_index, group } => {
                write!(f, "Node with index `{node_index}` not in group `{group}`")
            }
            Self::EdgeNotInGroup { edge_index, group } => {
                write!(f, "Edge with index `{edge_index}` not in group `{group}`")
            }
            Self::NodeAttributeNotFound {
                node_index,
                attribute,
            } => write!(
                f,
                "Attribute `{attribute}` does not exist on node `{node_index}`"
            ),
            Self::EdgeAttributeNotFound {
                edge_index,
                attribute,
            } => write!(
                f,
                "Attribute `{attribute}` does not exist on edge `{edge_index}`"
            ),
            Self::IncompatibleValueOperands {
                operation,
                left,
                right,
            } => match operation {
                ValueOperation::Add => write!(f, "Cannot add `{right}` to `{left}`"),
                ValueOperation::Subtract => write!(f, "Cannot subtract `{right}` from `{left}`"),
                ValueOperation::Multiply => write!(f, "Cannot multiply `{left}` with `{right}`"),
                ValueOperation::Divide => write!(f, "Cannot divide `{left}` by `{right}`"),
                ValueOperation::Power => {
                    write!(f, "Cannot raise `{left}` to the power of `{right}`")
                }
                ValueOperation::Modulo => write!(f, "Cannot mod `{left}` with `{right}`"),
            },
            Self::IncompatibleAttributeOperands {
                operation,
                left,
                right,
            } => match operation {
                ValueOperation::Add => write!(f, "Cannot add `{right}` to `{left}`"),
                ValueOperation::Subtract => write!(f, "Cannot subtract `{right}` from `{left}`"),
                ValueOperation::Multiply => write!(f, "Cannot multiply `{left}` with `{right}`"),
                ValueOperation::Divide => write!(f, "Cannot divide `{left}` by `{right}`"),
                ValueOperation::Power => {
                    write!(f, "Cannot raise `{left}` to the power of `{right}`")
                }
                ValueOperation::Modulo => write!(f, "Cannot mod `{left}` with `{right}`"),
            },
            Self::InvalidTimestamp => write!(f, "Invalid timestamp"),
            Self::PluginNotFound { name } => {
                write!(f, "Plugin with name `{name}` does not exist")
            }
            Self::PluginAlreadyExists { name } => {
                write!(f, "Plugin with name `{name}` already exists")
            }
            Self::PluginFailure { message } => write!(f, "Plugin failed: {message}"),
            Self::ConnectorFailure { message } => write!(f, "Connector failed: {message}"),
            Self::Schema(error) => write!(f, "{error}"),
            Self::Conversion(error) => write!(f, "{error}"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{ConversionError, GraphRecordError, SchemaError, ValueOperation};
    use crate::graphrecord::GraphRecordValue;

    #[test]
    fn test_display_entities() {
        assert_eq!(
            "Cannot find node with index `\"test\"`",
            GraphRecordError::NodeNotFound {
                node_index: "test".into()
            }
            .to_string()
        );
        assert_eq!(
            "Cannot find edge with index `0`",
            GraphRecordError::EdgeNotFound { edge_index: 0 }.to_string()
        );
        assert_eq!(
            "Node with index `\"test\"` already exists",
            GraphRecordError::NodeAlreadyExists {
                node_index: "test".into()
            }
            .to_string()
        );
        assert_eq!(
            "Cannot find group `\"test\"`",
            GraphRecordError::GroupNotFound {
                group: "test".into()
            }
            .to_string()
        );
        assert_eq!(
            "Group `\"test\"` already exists",
            GraphRecordError::GroupAlreadyExists {
                group: "test".into()
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_group_membership() {
        assert_eq!(
            "Node with index `\"test\"` already in group `\"group\"`",
            GraphRecordError::NodeAlreadyInGroup {
                node_index: "test".into(),
                group: "group".into()
            }
            .to_string()
        );
        assert_eq!(
            "Edge with index `0` already in group `\"group\"`",
            GraphRecordError::EdgeAlreadyInGroup {
                edge_index: 0,
                group: "group".into()
            }
            .to_string()
        );
        assert_eq!(
            "Node with index `\"test\"` not in group `\"group\"`",
            GraphRecordError::NodeNotInGroup {
                node_index: "test".into(),
                group: "group".into()
            }
            .to_string()
        );
        assert_eq!(
            "Edge with index `0` not in group `\"group\"`",
            GraphRecordError::EdgeNotInGroup {
                edge_index: 0,
                group: "group".into()
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_attributes() {
        assert_eq!(
            "Attribute `\"attribute\"` does not exist on node `\"test\"`",
            GraphRecordError::NodeAttributeNotFound {
                node_index: "test".into(),
                attribute: "attribute".into()
            }
            .to_string()
        );
        assert_eq!(
            "Attribute `\"attribute\"` does not exist on edge `0`",
            GraphRecordError::EdgeAttributeNotFound {
                edge_index: 0,
                attribute: "attribute".into()
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_operands() {
        let error = |operation| GraphRecordError::IncompatibleValueOperands {
            operation,
            left: GraphRecordValue::Int(1),
            right: GraphRecordValue::Bool(true),
        };

        assert_eq!(
            "Cannot add `true` to `1`",
            error(ValueOperation::Add).to_string()
        );
        assert_eq!(
            "Cannot subtract `true` from `1`",
            error(ValueOperation::Subtract).to_string()
        );
        assert_eq!(
            "Cannot multiply `1` with `true`",
            error(ValueOperation::Multiply).to_string()
        );
        assert_eq!(
            "Cannot divide `1` by `true`",
            error(ValueOperation::Divide).to_string()
        );
        assert_eq!(
            "Cannot raise `1` to the power of `true`",
            error(ValueOperation::Power).to_string()
        );
        assert_eq!(
            "Cannot mod `1` with `true`",
            error(ValueOperation::Modulo).to_string()
        );
        assert_eq!(
            "Cannot add `\"attribute\"` to `1`",
            GraphRecordError::IncompatibleAttributeOperands {
                operation: ValueOperation::Add,
                left: 1.into(),
                right: "attribute".into()
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_remaining() {
        assert_eq!(
            "Plugin failed: message",
            GraphRecordError::PluginFailure {
                message: "message".to_string()
            }
            .to_string()
        );
        assert_eq!(
            "Connector failed: message",
            GraphRecordError::ConnectorFailure {
                message: "message".to_string()
            }
            .to_string()
        );
        assert_eq!(
            "Invalid timestamp",
            GraphRecordError::InvalidTimestamp.to_string()
        );
        assert_eq!(
            "Plugin with name `\"plugin\"` does not exist",
            GraphRecordError::PluginNotFound {
                name: "plugin".into()
            }
            .to_string()
        );
        assert_eq!(
            "Plugin with name `\"plugin\"` already exists",
            GraphRecordError::PluginAlreadyExists {
                name: "plugin".into()
            }
            .to_string()
        );
        assert_eq!(
            "Group `\"test\"` is not defined in the schema",
            GraphRecordError::Schema(SchemaError::GroupNotInSchema {
                group: "test".into()
            })
            .to_string()
        );
        assert_eq!(
            "Cannot convert `true` into `GraphRecordAttribute`",
            GraphRecordError::Conversion(ConversionError::ValueToAttribute {
                value: GraphRecordValue::Bool(true)
            })
            .to_string()
        );
    }
}
