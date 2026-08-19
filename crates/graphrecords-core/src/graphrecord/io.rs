use crate::{
    errors::{GraphRecordError, GraphRecordResult, IoError},
    graphrecord::{GraphRecord, state::GraphState},
};
use std::{path::Path, sync::Arc};

impl GraphRecord {
    pub fn to_ron(&self, path: impl AsRef<Path>) -> GraphRecordResult<()> {
        let path = path.as_ref();

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                GraphRecordError::Io(IoError::DirectoryCreation {
                    path: parent.display().to_string(),
                    kind: error.kind(),
                })
            })?;
        }

        let contents = ron::to_string(self.state.as_ref()).map_err(|_| {
            GraphRecordError::Io(IoError::FileWrite {
                path: path.display().to_string(),
                kind: std::io::ErrorKind::InvalidData,
            })
        })?;

        std::fs::write(path, contents).map_err(|error| {
            GraphRecordError::Io(IoError::FileWrite {
                path: path.display().to_string(),
                kind: error.kind(),
            })
        })
    }

    pub fn from_ron(path: impl AsRef<Path>) -> GraphRecordResult<Self> {
        let path = path.as_ref();

        let contents = std::fs::read_to_string(path).map_err(|error| {
            GraphRecordError::Io(IoError::FileRead {
                path: path.display().to_string(),
                kind: error.kind(),
            })
        })?;

        let corrupted_file_error = || {
            GraphRecordError::Io(IoError::CorruptedFile {
                path: path.display().to_string(),
            })
        };

        let mut state: GraphState = ron::from_str(&contents).map_err(|_| corrupted_file_error())?;

        if !state.is_referentially_consistent() {
            return Err(corrupted_file_error());
        }

        state.rebuild_dictionaries();

        Ok(Self {
            state: Arc::new(state),
            #[cfg(feature = "plugins")]
            plugins: Arc::new(Vec::new()),
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        errors::{GraphRecordError, IoError},
        graphrecord::{
            AttributeMap, EdgeIndex, GraphRecord, Group, NodeIndex, Value,
            state::{EdgeEndpoints, NodeAddress},
        },
    };
    use chrono::{NaiveDateTime, TimeDelta};
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    static UNIQUE_PATH_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempPath {
        path: PathBuf,
    }

    impl TempPath {
        fn new(label: &str) -> Self {
            let unique = UNIQUE_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("graphrecords-io-test-{label}-{unique}"));

            Self { path }
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn create_rich_graphrecord() -> (GraphRecord, EdgeIndex) {
        let graphrecord = GraphRecord::new()
            .add_node(
                NodeIndex::from(1_i64),
                AttributeMap::from([
                    ("lorem".into(), "ipsum dolor sit amet".into()),
                    ("count".into(), 42.into()),
                    ("score".into(), 3.5.into()),
                    ("active".into(), true.into()),
                    ("created".into(), Value::DateTime(NaiveDateTime::MIN)),
                    ("span".into(), Value::Duration(TimeDelta::seconds(5))),
                    ("nothing".into(), Value::Null),
                    ("blank".into(), "".into()),
                ]),
            )
            .unwrap()
            .add_node(
                "consectetur".into(),
                AttributeMap::from([("count".into(), "text".into())]),
            )
            .unwrap()
            .add_node("adipiscing".into(), AttributeMap::new())
            .unwrap();

        let graphrecord = graphrecord
            .add_edge(
                NodeIndex::from(1_i64),
                "consectetur".into(),
                AttributeMap::from([("weight".into(), 1.5.into())]),
            )
            .unwrap();
        let grouped_edge_index = graphrecord.edge_indices().next().unwrap();

        let graphrecord = graphrecord
            .add_edge(
                "adipiscing".into(),
                "adipiscing".into(),
                AttributeMap::new(),
            )
            .unwrap();

        let graphrecord = graphrecord.add_group("elit".into()).unwrap();
        let graphrecord = graphrecord
            .add_nodes_to_group("elit".into(), vec![NodeIndex::from(1_i64)])
            .unwrap();
        let graphrecord = graphrecord
            .add_edges_to_group("elit".into(), vec![grouped_edge_index])
            .unwrap();

        (graphrecord, grouped_edge_index)
    }

    #[test]
    fn test_to_ron() {
        let (graphrecord, _) = create_rich_graphrecord();
        let temp_path = TempPath::new("to-ron");

        graphrecord.to_ron(&temp_path.path).unwrap();

        assert!(temp_path.path.is_file());

        let nested_path = TempPath::new("to-ron-nested");
        let file_path = nested_path.path.join("nested").join("graphrecord.ron");

        graphrecord.to_ron(&file_path).unwrap();

        assert!(file_path.is_file());
    }

    #[test]
    fn test_invalid_to_ron() {
        let graphrecord = GraphRecord::new();
        let existing_directory = TempPath::new("invalid-to-ron-directory");

        std::fs::create_dir_all(&existing_directory.path).unwrap();

        assert!(
            graphrecord
                .to_ron(&existing_directory.path)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Io(IoError::FileWrite { .. })
                ))
        );

        let blocking_file = TempPath::new("invalid-to-ron-blocking-file");

        std::fs::write(&blocking_file.path, "not a directory").unwrap();

        let unreachable_path = blocking_file.path.join("nested").join("graphrecord.ron");

        assert!(
            graphrecord
                .to_ron(&unreachable_path)
                .is_err_and(|error| matches!(
                    error,
                    GraphRecordError::Io(IoError::DirectoryCreation { .. })
                ))
        );
    }

    #[test]
    fn test_from_ron() {
        let (graphrecord, grouped_edge_index) = create_rich_graphrecord();
        let ungrouped_edge_index = graphrecord
            .edge_indices()
            .find(|edge_index| *edge_index != grouped_edge_index)
            .unwrap();
        let temp_path = TempPath::new("from-ron");

        graphrecord.to_ron(&temp_path.path).unwrap();

        let loaded_graphrecord = GraphRecord::from_ron(&temp_path.path).unwrap();

        assert_eq!(graphrecord.node_count(), loaded_graphrecord.node_count());
        assert_eq!(graphrecord.edge_count(), loaded_graphrecord.edge_count());
        assert_eq!(graphrecord.group_count(), loaded_graphrecord.group_count());
        assert_eq!(graphrecord.schema(), loaded_graphrecord.schema());

        assert!(loaded_graphrecord.contains_edge(&grouped_edge_index));
        assert!(loaded_graphrecord.contains_edge(&ungrouped_edge_index));

        assert_eq!(
            Some("\"ipsum dolor sit amet\"".to_string()),
            loaded_graphrecord
                .node_attribute(&NodeIndex::from(1_i64), &"lorem".into())
                .map(|value| value.to_string())
        );
        assert_eq!(
            Some(Value::Null.to_string()),
            loaded_graphrecord
                .node_attribute(&NodeIndex::from(1_i64), &"nothing".into())
                .map(|value| value.to_string())
        );
        assert_eq!(
            Some(1.5.to_string()),
            loaded_graphrecord
                .edge_attribute(&grouped_edge_index, &"weight".into())
                .map(|value| value.to_string())
        );

        assert!(loaded_graphrecord.contains_group(&Group::from("elit")));
    }

    #[test]
    fn test_invalid_from_ron() {
        let missing_path = TempPath::new("invalid-from-ron-missing");

        assert!(
            GraphRecord::from_ron(&missing_path.path).is_err_and(|error| matches!(
                error,
                GraphRecordError::Io(IoError::FileRead { .. })
            ))
        );

        let syntactically_corrupt_path = TempPath::new("invalid-from-ron-syntax");

        std::fs::write(&syntactically_corrupt_path.path, "not valid ron {{{").unwrap();

        assert!(
            GraphRecord::from_ron(&syntactically_corrupt_path.path).is_err_and(|error| matches!(
                error,
                GraphRecordError::Io(IoError::CorruptedFile { .. })
            ))
        );

        let (graphrecord, _) = create_rich_graphrecord();
        let mut state = (**graphrecord.state()).clone();
        let edge_address = state.edge_addresses().next().unwrap();

        state
            .edge_endpoints
            .get_mut_or_default(edge_address.chunk_index())
            .set(
                edge_address.chunk_local_address(),
                EdgeEndpoints {
                    source_address: NodeAddress::new(999),
                    target_address: NodeAddress::new(999),
                },
            );

        let semantically_corrupt_path = TempPath::new("invalid-from-ron-semantics");
        let corrupt_contents = ron::to_string(&state).unwrap();

        std::fs::write(&semantically_corrupt_path.path, corrupt_contents).unwrap();

        assert!(
            GraphRecord::from_ron(&semantically_corrupt_path.path).is_err_and(|error| matches!(
                error,
                GraphRecordError::Io(IoError::CorruptedFile { .. })
            ))
        );
    }
}
