#[cfg(feature = "io")]
use super::io::IoError;
use super::{conversion::ConversionError, schema::SchemaError};
#[cfg(feature = "plugins")]
use crate::graphrecord::PluginName;
use crate::graphrecord::{AttributeName, EdgeIndex, GroupIndex, Identifier, NodeIndex, Value};
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
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

#[derive(Debug, Clone)]
pub enum GraphRecordError {
    NodeNotFound {
        node_index: NodeIndex,
    },
    NodesNotFound {
        node_indices: Vec<NodeIndex>,
    },
    EdgeNotFound {
        edge_index: EdgeIndex,
    },
    EdgesNotFound {
        edge_indices: Vec<EdgeIndex>,
    },
    NodeAlreadyExists {
        node_index: NodeIndex,
    },
    AddressSpaceExhausted,
    GroupNotFound {
        group_index: GroupIndex,
    },
    GroupsNotFound {
        group_indices: Vec<GroupIndex>,
    },
    GroupAlreadyExists {
        group_index: GroupIndex,
    },
    NoNodeSelected,
    NoEdgeSelected,
    NoGroupSelected,
    NodeAlreadyInGroup {
        node_index: NodeIndex,
        group_index: GroupIndex,
    },
    EdgeAlreadyInGroup {
        edge_index: EdgeIndex,
        group_index: GroupIndex,
    },
    NodeNotInGroup {
        node_index: NodeIndex,
        group_index: GroupIndex,
    },
    EdgeNotInGroup {
        edge_index: EdgeIndex,
        group_index: GroupIndex,
    },
    NodeAttributeNotFound {
        node_index: NodeIndex,
        attribute_name: AttributeName,
    },
    EdgeAttributeNotFound {
        edge_index: EdgeIndex,
        attribute_name: AttributeName,
    },
    NodeAttributeConflict {
        node_index: NodeIndex,
        attribute_name: AttributeName,
        self_value: Value,
        other_value: Value,
    },
    IncompatibleValueOperands {
        operation: ValueOperation,
        left: Value,
        right: Value,
    },
    IncompatibleIdentifierOperands {
        operation: ValueOperation,
        left: Identifier,
        right: Identifier,
    },
    InvalidTimestamp,
    #[cfg(feature = "plugins")]
    PluginNotFound {
        name: PluginName,
    },
    #[cfg(feature = "plugins")]
    PluginAlreadyExists {
        name: PluginName,
    },
    #[cfg(feature = "plugins")]
    PluginFailure {
        cause: Arc<dyn Error + Send + Sync>,
    },
    WriterFailure {
        cause: Arc<dyn Error + Send + Sync>,
    },
    QueryFailure {
        cause: Arc<dyn Error + Send + Sync>,
    },
    Schema(SchemaError),
    Conversion(ConversionError),
    #[cfg(feature = "io")]
    Io(IoError),
}

impl Error for GraphRecordError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            #[cfg(feature = "plugins")]
            Self::PluginFailure { cause } => Some(&**cause),
            Self::WriterFailure { cause } | Self::QueryFailure { cause } => Some(&**cause),
            Self::Schema(error) => Some(error),
            Self::Conversion(error) => Some(error),
            #[cfg(feature = "io")]
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

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

#[cfg(feature = "io")]
impl From<IoError> for GraphRecordError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl Display for GraphRecordError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NodeNotFound { node_index } => {
                write!(f, "Cannot find node with index `{node_index}`")
            }
            Self::NodesNotFound { node_indices } => {
                let indices = node_indices
                    .iter()
                    .map(|node_index| format!("`{node_index}`"))
                    .collect::<Vec<_>>()
                    .join(", ");

                write!(f, "Cannot find nodes with indices {indices}")
            }
            Self::EdgeNotFound { edge_index } => {
                write!(f, "Cannot find edge with index `{edge_index}`")
            }
            Self::EdgesNotFound { edge_indices } => {
                let indices = edge_indices
                    .iter()
                    .map(|edge_index| format!("`{edge_index}`"))
                    .collect::<Vec<_>>()
                    .join(", ");

                write!(f, "Cannot find edges with indices {indices}")
            }
            Self::NodeAlreadyExists { node_index } => {
                write!(f, "Node with index `{node_index}` already exists")
            }
            Self::AddressSpaceExhausted => {
                write!(f, "Address space is exhausted")
            }
            Self::GroupNotFound { group_index } => {
                write!(f, "Cannot find group with index `{group_index}`")
            }
            Self::GroupsNotFound { group_indices } => {
                let indices = group_indices
                    .iter()
                    .map(|group_index| format!("`{group_index}`"))
                    .collect::<Vec<_>>()
                    .join(", ");

                write!(f, "Cannot find groups with indices {indices}")
            }
            Self::GroupAlreadyExists { group_index } => {
                write!(f, "Group with index `{group_index}` already exists")
            }
            Self::NoNodeSelected => write!(f, "No node selected"),
            Self::NoEdgeSelected => write!(f, "No edge selected"),
            Self::NoGroupSelected => write!(f, "No group selected"),
            Self::NodeAlreadyInGroup {
                node_index,
                group_index,
            } => {
                write!(
                    f,
                    "Node with index `{node_index}` already in group `{group_index}`"
                )
            }
            Self::EdgeAlreadyInGroup {
                edge_index,
                group_index,
            } => {
                write!(
                    f,
                    "Edge with index `{edge_index}` already in group `{group_index}`"
                )
            }
            Self::NodeNotInGroup {
                node_index,
                group_index,
            } => {
                write!(
                    f,
                    "Node with index `{node_index}` not in group `{group_index}`"
                )
            }
            Self::EdgeNotInGroup {
                edge_index,
                group_index,
            } => {
                write!(
                    f,
                    "Edge with index `{edge_index}` not in group `{group_index}`"
                )
            }
            Self::NodeAttributeNotFound {
                node_index,
                attribute_name,
            } => write!(
                f,
                "Attribute `{attribute_name}` does not exist on node `{node_index}`"
            ),
            Self::EdgeAttributeNotFound {
                edge_index,
                attribute_name,
            } => write!(
                f,
                "Attribute `{attribute_name}` does not exist on edge `{edge_index}`"
            ),
            Self::NodeAttributeConflict {
                node_index,
                attribute_name,
                self_value,
                other_value,
            } => write!(
                f,
                "Attribute `{attribute_name}` on node `{node_index}` conflicts between `{self_value}` and `{other_value}`"
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
                ValueOperation::Modulo => write!(f, "Cannot take `{left}` modulo `{right}`"),
            },
            Self::IncompatibleIdentifierOperands {
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
                ValueOperation::Modulo => write!(f, "Cannot take `{left}` modulo `{right}`"),
            },
            Self::InvalidTimestamp => write!(f, "Invalid timestamp"),
            #[cfg(feature = "plugins")]
            Self::PluginNotFound { name } => {
                write!(f, "Plugin with name `{name}` does not exist")
            }
            #[cfg(feature = "plugins")]
            Self::PluginAlreadyExists { name } => {
                write!(f, "Plugin with name `{name}` already exists")
            }
            #[cfg(feature = "plugins")]
            Self::PluginFailure { cause } => write!(f, "Plugin failed: {cause}"),
            Self::WriterFailure { cause } => write!(f, "Writer failed: {cause}"),
            Self::QueryFailure { cause } => write!(f, "Query failed: {cause}"),
            Self::Schema(error) => write!(f, "{error}"),
            Self::Conversion(error) => write!(f, "{error}"),
            #[cfg(feature = "io")]
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

#[cfg(test)]
mod test {
    #[cfg(feature = "io")]
    use super::IoError;
    use super::{ConversionError, GraphRecordError, SchemaError, ValueOperation};
    use crate::graphrecord::{EdgeIndex, Value};
    use std::{
        error::Error,
        fmt::{Display, Formatter, Result as FmtResult},
        sync::Arc,
    };

    #[derive(Debug)]
    struct LoremError;

    impl Display for LoremError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "lorem ipsum")
        }
    }

    impl Error for LoremError {}

    #[test]
    fn test_source() {
        let writer_failure = GraphRecordError::WriterFailure {
            cause: Arc::new(LoremError),
        };

        assert!(
            writer_failure
                .source()
                .unwrap()
                .downcast_ref::<LoremError>()
                .is_some()
        );

        let query_failure = GraphRecordError::QueryFailure {
            cause: Arc::new(LoremError),
        };

        assert!(
            query_failure
                .source()
                .unwrap()
                .downcast_ref::<LoremError>()
                .is_some()
        );

        let schema_error = GraphRecordError::Schema(SchemaError::GroupNotInSchema {
            group_index: "test".into(),
        });

        assert_eq!(
            Some(&SchemaError::GroupNotInSchema {
                group_index: "test".into()
            }),
            schema_error.source().unwrap().downcast_ref::<SchemaError>()
        );

        let conversion_error = GraphRecordError::Conversion(ConversionError::ValueToIdentifier {
            value: Value::Bool(true),
        });

        assert_eq!(
            Some(&ConversionError::ValueToIdentifier {
                value: Value::Bool(true)
            }),
            conversion_error
                .source()
                .unwrap()
                .downcast_ref::<ConversionError>()
        );

        #[cfg(feature = "io")]
        {
            let io_error = GraphRecordError::Io(IoError::CorruptedFile {
                path: "path".to_string(),
            });

            assert_eq!(
                Some(&IoError::CorruptedFile {
                    path: "path".to_string()
                }),
                io_error.source().unwrap().downcast_ref::<IoError>()
            );
        }

        let node_not_found = GraphRecordError::NodeNotFound {
            node_index: "test".into(),
        };

        assert!(node_not_found.source().is_none());
    }

    #[cfg(feature = "plugins")]
    #[test]
    fn test_source_plugins() {
        let plugin_failure = GraphRecordError::PluginFailure {
            cause: Arc::new(LoremError),
        };

        assert!(
            plugin_failure
                .source()
                .unwrap()
                .downcast_ref::<LoremError>()
                .is_some()
        );
    }

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
            "Cannot find nodes with indices `\"test\"`, `\"other\"`",
            GraphRecordError::NodesNotFound {
                node_indices: vec!["test".into(), "other".into()]
            }
            .to_string()
        );
        assert_eq!(
            "Cannot find edge with index `0000000000000000:0`",
            GraphRecordError::EdgeNotFound {
                edge_index: EdgeIndex::new(0, 0)
            }
            .to_string()
        );
        assert_eq!(
            "Cannot find edges with indices `0000000000000000:0`",
            GraphRecordError::EdgesNotFound {
                edge_indices: vec![EdgeIndex::new(0, 0)]
            }
            .to_string()
        );
        assert_eq!(
            "Node with index `\"test\"` already exists",
            GraphRecordError::NodeAlreadyExists {
                node_index: "test".into()
            }
            .to_string()
        );
        assert_eq!(
            "Address space is exhausted",
            GraphRecordError::AddressSpaceExhausted.to_string()
        );
        assert_eq!(
            "Cannot find group with index `\"test\"`",
            GraphRecordError::GroupNotFound {
                group_index: "test".into()
            }
            .to_string()
        );
        assert_eq!(
            "Cannot find groups with indices `\"test\"`, `\"other\"`",
            GraphRecordError::GroupsNotFound {
                group_indices: vec!["test".into(), "other".into()]
            }
            .to_string()
        );
        assert_eq!(
            "Group with index `\"test\"` already exists",
            GraphRecordError::GroupAlreadyExists {
                group_index: "test".into()
            }
            .to_string()
        );
        assert_eq!(
            "No node selected",
            GraphRecordError::NoNodeSelected.to_string()
        );
        assert_eq!(
            "No edge selected",
            GraphRecordError::NoEdgeSelected.to_string()
        );
        assert_eq!(
            "No group selected",
            GraphRecordError::NoGroupSelected.to_string()
        );
    }

    #[test]
    fn test_display_group_membership() {
        assert_eq!(
            "Node with index `\"test\"` already in group `\"group\"`",
            GraphRecordError::NodeAlreadyInGroup {
                node_index: "test".into(),
                group_index: "group".into()
            }
            .to_string()
        );
        assert_eq!(
            "Edge with index `0000000000000000:0` already in group `\"group\"`",
            GraphRecordError::EdgeAlreadyInGroup {
                edge_index: EdgeIndex::new(0, 0),
                group_index: "group".into()
            }
            .to_string()
        );
        assert_eq!(
            "Node with index `\"test\"` not in group `\"group\"`",
            GraphRecordError::NodeNotInGroup {
                node_index: "test".into(),
                group_index: "group".into()
            }
            .to_string()
        );
        assert_eq!(
            "Edge with index `0000000000000000:0` not in group `\"group\"`",
            GraphRecordError::EdgeNotInGroup {
                edge_index: EdgeIndex::new(0, 0),
                group_index: "group".into()
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
                attribute_name: "attribute".into()
            }
            .to_string()
        );
        assert_eq!(
            "Attribute `\"attribute\"` does not exist on edge `0000000000000000:0`",
            GraphRecordError::EdgeAttributeNotFound {
                edge_index: EdgeIndex::new(0, 0),
                attribute_name: "attribute".into()
            }
            .to_string()
        );
        assert_eq!(
            "Attribute `\"attribute\"` on node `\"test\"` conflicts between `1` and `2`",
            GraphRecordError::NodeAttributeConflict {
                node_index: "test".into(),
                attribute_name: "attribute".into(),
                self_value: Value::Int(1),
                other_value: Value::Int(2)
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_operands() {
        let error = |operation| GraphRecordError::IncompatibleValueOperands {
            operation,
            left: Value::Int(1),
            right: Value::Bool(true),
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
            "Cannot take `1` modulo `true`",
            error(ValueOperation::Modulo).to_string()
        );
        assert_eq!(
            "Cannot add `\"attribute\"` to `1`",
            GraphRecordError::IncompatibleIdentifierOperands {
                operation: ValueOperation::Add,
                left: 1.into(),
                right: "attribute".into()
            }
            .to_string()
        );
    }

    #[cfg(feature = "plugins")]
    #[test]
    fn test_display_plugins() {
        assert_eq!(
            "Plugin failed: lorem ipsum",
            GraphRecordError::PluginFailure {
                cause: Arc::new(LoremError)
            }
            .to_string()
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
    }

    #[test]
    fn test_display_writers() {
        assert_eq!(
            "Writer failed: lorem ipsum",
            GraphRecordError::WriterFailure {
                cause: Arc::new(LoremError)
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_query() {
        assert_eq!(
            "Query failed: lorem ipsum",
            GraphRecordError::QueryFailure {
                cause: Arc::new(LoremError)
            }
            .to_string()
        );
    }

    #[test]
    fn test_display_timestamp() {
        assert_eq!(
            "Invalid timestamp",
            GraphRecordError::InvalidTimestamp.to_string()
        );
    }

    #[test]
    fn test_display_wrapped() {
        assert_eq!(
            "Group with index `\"test\"` is not defined in the schema",
            GraphRecordError::Schema(SchemaError::GroupNotInSchema {
                group_index: "test".into()
            })
            .to_string()
        );
        assert_eq!(
            "Cannot convert `true` into `Identifier`",
            GraphRecordError::Conversion(ConversionError::ValueToIdentifier {
                value: Value::Bool(true)
            })
            .to_string()
        );
        #[cfg(feature = "io")]
        assert_eq!(
            "File `path` is corrupted",
            GraphRecordError::Io(IoError::CorruptedFile {
                path: "path".to_string()
            })
            .to_string()
        );
    }
}
