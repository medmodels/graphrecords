use super::{
    Changes, GraphRecord,
    changes::{
        AddEdges, AddEdgesInGroup, AddEdgesToGroup, AddGroup, AddNodes, AddNodesInGroup,
        AddNodesToGroup, Clear, FreezeSchema, RemoveEdgeAttributes, RemoveEdges,
        RemoveEdgesFromGroup, RemoveGroups, RemoveNodeAttributes, RemoveNodes,
        RemoveNodesFromGroup, ReplaceEdgeAttributes, ReplaceNodeAttributes, SetEdgeAttributes,
        SetNodeAttributes, SetSchema, UnfreezeSchema,
    },
};
use crate::errors::GraphRecordResult;

pub trait Plugin: Send + Sync {
    #[allow(unused_variables)]
    fn initialize(&self, record: &GraphRecord) -> GraphRecordResult<Changes> {
        Ok(Changes::new())
    }

    #[allow(unused_variables)]
    fn finalize(&self, record: &GraphRecord) -> GraphRecordResult<Changes> {
        Ok(Changes::new())
    }

    #[allow(unused_variables)]
    fn on_add_nodes(&self, record: &GraphRecord, addition: AddNodes) -> GraphRecordResult<Changes> {
        Ok(addition.into())
    }

    #[allow(unused_variables)]
    fn post_add_nodes(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_add_nodes_in_group(
        &self,
        record: &GraphRecord,
        addition: AddNodesInGroup,
    ) -> GraphRecordResult<Changes> {
        Ok(addition.into())
    }

    #[allow(unused_variables)]
    fn post_add_nodes_in_group(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_add_edges(&self, record: &GraphRecord, addition: AddEdges) -> GraphRecordResult<Changes> {
        Ok(addition.into())
    }

    #[allow(unused_variables)]
    fn post_add_edges(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_add_edges_in_group(
        &self,
        record: &GraphRecord,
        addition: AddEdgesInGroup,
    ) -> GraphRecordResult<Changes> {
        Ok(addition.into())
    }

    #[allow(unused_variables)]
    fn post_add_edges_in_group(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_remove_nodes(
        &self,
        record: &GraphRecord,
        removal: RemoveNodes,
    ) -> GraphRecordResult<Changes> {
        Ok(removal.into())
    }

    #[allow(unused_variables)]
    fn post_remove_nodes(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_remove_edges(
        &self,
        record: &GraphRecord,
        removal: RemoveEdges,
    ) -> GraphRecordResult<Changes> {
        Ok(removal.into())
    }

    #[allow(unused_variables)]
    fn post_remove_edges(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_set_node_attributes(
        &self,
        record: &GraphRecord,
        assignment: SetNodeAttributes,
    ) -> GraphRecordResult<Changes> {
        Ok(assignment.into())
    }

    #[allow(unused_variables)]
    fn post_set_node_attributes(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_replace_node_attributes(
        &self,
        record: &GraphRecord,
        assignment: ReplaceNodeAttributes,
    ) -> GraphRecordResult<Changes> {
        Ok(assignment.into())
    }

    #[allow(unused_variables)]
    fn post_replace_node_attributes(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_remove_node_attributes(
        &self,
        record: &GraphRecord,
        removal: RemoveNodeAttributes,
    ) -> GraphRecordResult<Changes> {
        Ok(removal.into())
    }

    #[allow(unused_variables)]
    fn post_remove_node_attributes(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_set_edge_attributes(
        &self,
        record: &GraphRecord,
        assignment: SetEdgeAttributes,
    ) -> GraphRecordResult<Changes> {
        Ok(assignment.into())
    }

    #[allow(unused_variables)]
    fn post_set_edge_attributes(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_replace_edge_attributes(
        &self,
        record: &GraphRecord,
        assignment: ReplaceEdgeAttributes,
    ) -> GraphRecordResult<Changes> {
        Ok(assignment.into())
    }

    #[allow(unused_variables)]
    fn post_replace_edge_attributes(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_remove_edge_attributes(
        &self,
        record: &GraphRecord,
        removal: RemoveEdgeAttributes,
    ) -> GraphRecordResult<Changes> {
        Ok(removal.into())
    }

    #[allow(unused_variables)]
    fn post_remove_edge_attributes(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_add_group(&self, record: &GraphRecord, addition: AddGroup) -> GraphRecordResult<Changes> {
        Ok(addition.into())
    }

    #[allow(unused_variables)]
    fn post_add_group(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_remove_groups(
        &self,
        record: &GraphRecord,
        removal: RemoveGroups,
    ) -> GraphRecordResult<Changes> {
        Ok(removal.into())
    }

    #[allow(unused_variables)]
    fn post_remove_groups(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_add_nodes_to_group(
        &self,
        record: &GraphRecord,
        membership: AddNodesToGroup,
    ) -> GraphRecordResult<Changes> {
        Ok(membership.into())
    }

    #[allow(unused_variables)]
    fn post_add_nodes_to_group(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_remove_nodes_from_group(
        &self,
        record: &GraphRecord,
        membership: RemoveNodesFromGroup,
    ) -> GraphRecordResult<Changes> {
        Ok(membership.into())
    }

    #[allow(unused_variables)]
    fn post_remove_nodes_from_group(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_add_edges_to_group(
        &self,
        record: &GraphRecord,
        membership: AddEdgesToGroup,
    ) -> GraphRecordResult<Changes> {
        Ok(membership.into())
    }

    #[allow(unused_variables)]
    fn post_add_edges_to_group(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_remove_edges_from_group(
        &self,
        record: &GraphRecord,
        membership: RemoveEdgesFromGroup,
    ) -> GraphRecordResult<Changes> {
        Ok(membership.into())
    }

    #[allow(unused_variables)]
    fn post_remove_edges_from_group(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_set_schema(
        &self,
        record: &GraphRecord,
        schema_change: SetSchema,
    ) -> GraphRecordResult<Changes> {
        Ok(schema_change.into())
    }

    #[allow(unused_variables)]
    fn post_set_schema(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_freeze_schema(
        &self,
        record: &GraphRecord,
        schema_change: FreezeSchema,
    ) -> GraphRecordResult<Changes> {
        Ok(schema_change.into())
    }

    #[allow(unused_variables)]
    fn post_freeze_schema(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_unfreeze_schema(
        &self,
        record: &GraphRecord,
        schema_change: UnfreezeSchema,
    ) -> GraphRecordResult<Changes> {
        Ok(schema_change.into())
    }

    #[allow(unused_variables)]
    fn post_unfreeze_schema(
        &self,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn on_clear(&self, record: &GraphRecord, clearing: Clear) -> GraphRecordResult<Changes> {
        Ok(clearing.into())
    }

    #[allow(unused_variables)]
    fn post_clear(&self, previous: &GraphRecord, candidate: &GraphRecord) -> GraphRecordResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{
        AddEdges, AddEdgesInGroup, AddEdgesToGroup, AddGroup, AddNodes, AddNodesInGroup,
        AddNodesToGroup, Changes, Clear, FreezeSchema, GraphRecord, Plugin, RemoveEdgeAttributes,
        RemoveEdges, RemoveEdgesFromGroup, RemoveGroups, RemoveNodeAttributes, RemoveNodes,
        RemoveNodesFromGroup, ReplaceEdgeAttributes, ReplaceNodeAttributes, SetEdgeAttributes,
        SetNodeAttributes, SetSchema, UnfreezeSchema,
    };
    use crate::{
        errors::{GraphRecordError, GraphRecordResult},
        graphrecord::{
            AttributeMap, GroupIndex, NodeBatch, NodeIndex, PluginName, Value, schema::Schema,
        },
    };
    use std::{
        error::Error,
        fmt::{Display, Formatter, Result as FmtResult},
        sync::{Arc, Mutex},
    };

    struct PassThroughPlugin;

    impl Plugin for PassThroughPlugin {}

    struct LifecyclePlugin;

    impl Plugin for LifecyclePlugin {
        fn initialize(&self, _record: &GraphRecord) -> GraphRecordResult<Changes> {
            Ok(AddGroup::new("dolor".into()).into())
        }

        fn finalize(&self, _record: &GraphRecord) -> GraphRecordResult<Changes> {
            Ok(RemoveGroups::new(vec!["dolor".into()]).into())
        }
    }

    struct RewritingPlugin;

    impl Plugin for RewritingPlugin {
        fn on_add_nodes(
            &self,
            _record: &GraphRecord,
            addition: AddNodes,
        ) -> GraphRecordResult<Changes> {
            let batch: Vec<_> = addition
                .batch()
                .iter()
                .map(|(node_index, attributes)| {
                    let mut attributes = attributes.clone();
                    attributes.insert("rewritten".into(), true.into());

                    (node_index.clone(), attributes)
                })
                .collect();

            Ok(AddNodes::new(NodeBatch::from(batch)).into())
        }
    }

    struct ExpandingPlugin {
        group_index: GroupIndex,
    }

    impl Plugin for ExpandingPlugin {
        fn on_add_nodes(
            &self,
            _record: &GraphRecord,
            addition: AddNodes,
        ) -> GraphRecordResult<Changes> {
            let mut changes = Changes::from(addition);
            changes.push(AddGroup::new(self.group_index.clone()));

            Ok(changes)
        }
    }

    struct SwallowingPlugin;

    impl Plugin for SwallowingPlugin {
        fn on_add_nodes(
            &self,
            _record: &GraphRecord,
            _addition: AddNodes,
        ) -> GraphRecordResult<Changes> {
            Ok(Changes::new())
        }
    }

    #[derive(Debug)]
    struct VetoError {
        label: &'static str,
    }

    impl Display for VetoError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "{}", self.label)
        }
    }

    impl Error for VetoError {}

    struct VetoingPlugin;

    impl Plugin for VetoingPlugin {
        fn on_add_nodes(
            &self,
            _record: &GraphRecord,
            _addition: AddNodes,
        ) -> GraphRecordResult<Changes> {
            Err(GraphRecordError::PluginFailure {
                cause: Arc::new(VetoError { label: "lorem" }),
            })
        }
    }

    struct PostVetoingPlugin;

    impl Plugin for PostVetoingPlugin {
        fn post_add_nodes(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            Err(GraphRecordError::PluginFailure {
                cause: Arc::new(VetoError { label: "ipsum" }),
            })
        }
    }

    struct RecordingPlugin {
        label: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl Plugin for RecordingPlugin {
        fn on_add_nodes(
            &self,
            _record: &GraphRecord,
            addition: AddNodes,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push(format!(
                "{}:on_add_nodes:len={}",
                self.label,
                addition.batch().len()
            ));

            Ok(addition.into())
        }

        fn post_add_nodes(
            &self,
            previous: &GraphRecord,
            candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push(format!(
                "{}:post_add_nodes:delta={}",
                self.label,
                candidate.node_count() - previous.node_count()
            ));

            Ok(())
        }

        fn on_add_group(
            &self,
            _record: &GraphRecord,
            addition: AddGroup,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:on_add_group", self.label));

            Ok(addition.into())
        }

        fn post_add_group(
            &self,
            previous: &GraphRecord,
            candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push(format!(
                "{}:post_add_group:delta={}",
                self.label,
                candidate.group_count() - previous.group_count()
            ));

            Ok(())
        }
    }

    struct TracingPlugin {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl Plugin for TracingPlugin {
        fn on_add_nodes(
            &self,
            _record: &GraphRecord,
            addition: AddNodes,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_add_nodes".into());

            Ok(addition.into())
        }

        fn post_add_nodes(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_add_nodes".into());

            Ok(())
        }

        fn on_add_nodes_in_group(
            &self,
            _record: &GraphRecord,
            addition: AddNodesInGroup,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_add_nodes_in_group".into());

            Ok(addition.into())
        }

        fn post_add_nodes_in_group(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_add_nodes_in_group".into());

            Ok(())
        }

        fn on_add_edges(
            &self,
            _record: &GraphRecord,
            addition: AddEdges,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_add_edges".into());

            Ok(addition.into())
        }

        fn post_add_edges(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_add_edges".into());

            Ok(())
        }

        fn on_add_edges_in_group(
            &self,
            _record: &GraphRecord,
            addition: AddEdgesInGroup,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_add_edges_in_group".into());

            Ok(addition.into())
        }

        fn post_add_edges_in_group(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_add_edges_in_group".into());

            Ok(())
        }

        fn on_remove_nodes(
            &self,
            _record: &GraphRecord,
            removal: RemoveNodes,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_remove_nodes".into());

            Ok(removal.into())
        }

        fn post_remove_nodes(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_remove_nodes".into());

            Ok(())
        }

        fn on_remove_edges(
            &self,
            _record: &GraphRecord,
            removal: RemoveEdges,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_remove_edges".into());

            Ok(removal.into())
        }

        fn post_remove_edges(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_remove_edges".into());

            Ok(())
        }

        fn on_set_node_attributes(
            &self,
            _record: &GraphRecord,
            assignment: SetNodeAttributes,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_set_node_attributes".into());

            Ok(assignment.into())
        }

        fn post_set_node_attributes(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_set_node_attributes".into());

            Ok(())
        }

        fn on_replace_node_attributes(
            &self,
            _record: &GraphRecord,
            assignment: ReplaceNodeAttributes,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_replace_node_attributes".into());

            Ok(assignment.into())
        }

        fn post_replace_node_attributes(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_replace_node_attributes".into());

            Ok(())
        }

        fn on_remove_node_attributes(
            &self,
            _record: &GraphRecord,
            removal: RemoveNodeAttributes,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_remove_node_attributes".into());

            Ok(removal.into())
        }

        fn post_remove_node_attributes(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_remove_node_attributes".into());

            Ok(())
        }

        fn on_set_edge_attributes(
            &self,
            _record: &GraphRecord,
            assignment: SetEdgeAttributes,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_set_edge_attributes".into());

            Ok(assignment.into())
        }

        fn post_set_edge_attributes(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_set_edge_attributes".into());

            Ok(())
        }

        fn on_replace_edge_attributes(
            &self,
            _record: &GraphRecord,
            assignment: ReplaceEdgeAttributes,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_replace_edge_attributes".into());

            Ok(assignment.into())
        }

        fn post_replace_edge_attributes(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_replace_edge_attributes".into());

            Ok(())
        }

        fn on_remove_edge_attributes(
            &self,
            _record: &GraphRecord,
            removal: RemoveEdgeAttributes,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_remove_edge_attributes".into());

            Ok(removal.into())
        }

        fn post_remove_edge_attributes(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_remove_edge_attributes".into());

            Ok(())
        }

        fn on_add_group(
            &self,
            _record: &GraphRecord,
            addition: AddGroup,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_add_group".into());

            Ok(addition.into())
        }

        fn post_add_group(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_add_group".into());

            Ok(())
        }

        fn on_remove_groups(
            &self,
            _record: &GraphRecord,
            removal: RemoveGroups,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_remove_groups".into());

            Ok(removal.into())
        }

        fn post_remove_groups(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_remove_groups".into());

            Ok(())
        }

        fn on_add_nodes_to_group(
            &self,
            _record: &GraphRecord,
            membership: AddNodesToGroup,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_add_nodes_to_group".into());

            Ok(membership.into())
        }

        fn post_add_nodes_to_group(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_add_nodes_to_group".into());

            Ok(())
        }

        fn on_remove_nodes_from_group(
            &self,
            _record: &GraphRecord,
            membership: RemoveNodesFromGroup,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_remove_nodes_from_group".into());

            Ok(membership.into())
        }

        fn post_remove_nodes_from_group(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_remove_nodes_from_group".into());

            Ok(())
        }

        fn on_add_edges_to_group(
            &self,
            _record: &GraphRecord,
            membership: AddEdgesToGroup,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_add_edges_to_group".into());

            Ok(membership.into())
        }

        fn post_add_edges_to_group(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_add_edges_to_group".into());

            Ok(())
        }

        fn on_remove_edges_from_group(
            &self,
            _record: &GraphRecord,
            membership: RemoveEdgesFromGroup,
        ) -> GraphRecordResult<Changes> {
            self.log
                .lock()
                .unwrap()
                .push("on_remove_edges_from_group".into());

            Ok(membership.into())
        }

        fn post_remove_edges_from_group(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log
                .lock()
                .unwrap()
                .push("post_remove_edges_from_group".into());

            Ok(())
        }

        fn on_set_schema(
            &self,
            _record: &GraphRecord,
            schema_change: SetSchema,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_set_schema".into());

            Ok(schema_change.into())
        }

        fn post_set_schema(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_set_schema".into());

            Ok(())
        }

        fn on_freeze_schema(
            &self,
            _record: &GraphRecord,
            schema_change: FreezeSchema,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_freeze_schema".into());

            Ok(schema_change.into())
        }

        fn post_freeze_schema(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_freeze_schema".into());

            Ok(())
        }

        fn on_unfreeze_schema(
            &self,
            _record: &GraphRecord,
            schema_change: UnfreezeSchema,
        ) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_unfreeze_schema".into());

            Ok(schema_change.into())
        }

        fn post_unfreeze_schema(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_unfreeze_schema".into());

            Ok(())
        }

        fn on_clear(&self, _record: &GraphRecord, clearing: Clear) -> GraphRecordResult<Changes> {
            self.log.lock().unwrap().push("on_clear".into());

            Ok(clearing.into())
        }

        fn post_clear(
            &self,
            _previous: &GraphRecord,
            _candidate: &GraphRecord,
        ) -> GraphRecordResult<()> {
            self.log.lock().unwrap().push("post_clear".into());

            Ok(())
        }
    }

    #[test]
    fn test_dispatch() {
        let graphrecord = GraphRecord::new()
            .add_plugin("noop", PassThroughPlugin)
            .unwrap()
            .add_node("lorem", AttributeMap::new())
            .unwrap();

        assert_eq!(1, graphrecord.node_count());
        assert!(graphrecord.contains_node("lorem"));

        let log = Arc::new(Mutex::new(Vec::new()));
        let graphrecord = GraphRecord::new()
            .add_plugin(
                "first",
                RecordingPlugin {
                    label: "first",
                    log: Arc::clone(&log),
                },
            )
            .unwrap()
            .add_plugin(
                "second",
                RecordingPlugin {
                    label: "second",
                    log: Arc::clone(&log),
                },
            )
            .unwrap();

        graphrecord.add_node("lorem", AttributeMap::new()).unwrap();

        assert_eq!(
            Some(&"first:on_add_nodes:len=1".to_string()),
            log.lock().unwrap().first()
        );
        assert_eq!(
            Some(&"second:on_add_nodes:len=1".to_string()),
            log.lock().unwrap().get(1)
        );

        let log = Arc::new(Mutex::new(Vec::new()));
        let graphrecord = GraphRecord::new()
            .add_plugin(
                "recorder",
                RecordingPlugin {
                    label: "recorder",
                    log: Arc::clone(&log),
                },
            )
            .unwrap();

        graphrecord
            .add_nodes(Vec::<(NodeIndex, AttributeMap)>::new())
            .unwrap();

        assert_eq!(
            Some(&"recorder:on_add_nodes:len=0".to_string()),
            log.lock().unwrap().first()
        );
    }

    #[test]
    fn test_dispatch_rewriting() {
        let graphrecord = GraphRecord::new()
            .add_plugin("rewriter", RewritingPlugin)
            .unwrap()
            .add_node("lorem", AttributeMap::new())
            .unwrap();

        assert_eq!(
            Some(Value::from(true)),
            graphrecord
                .node("lorem")
                .unwrap()
                .attribute("rewritten")
                .map(Value::from)
        );
    }

    #[test]
    fn test_dispatch_expansion() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let graphrecord = GraphRecord::new()
            .add_plugin(
                "expander",
                ExpandingPlugin {
                    group_index: "derived".into(),
                },
            )
            .unwrap()
            .add_plugin(
                "recorder",
                RecordingPlugin {
                    label: "recorder",
                    log: Arc::clone(&log),
                },
            )
            .unwrap();

        let derived = graphrecord.add_node("lorem", AttributeMap::new()).unwrap();

        assert_eq!(1, derived.node_count());
        assert!(derived.contains_node("lorem"));
        assert_eq!(1, derived.group_count());
        assert!(derived.contains_group("derived"));
        assert_eq!(
            vec![
                "recorder:on_add_nodes:len=1".to_string(),
                "recorder:on_add_group".to_string(),
                "recorder:post_add_nodes:delta=1".to_string(),
                "recorder:post_add_group:delta=1".to_string(),
            ],
            *log.lock().unwrap()
        );
    }

    #[test]
    fn test_dispatch_swallowing() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let with_plugins = GraphRecord::new()
            .add_node("existing", AttributeMap::new())
            .unwrap()
            .add_plugin("swallower", SwallowingPlugin)
            .unwrap()
            .add_plugin(
                "second",
                RecordingPlugin {
                    label: "second",
                    log: Arc::clone(&log),
                },
            )
            .unwrap();

        let derived = with_plugins.add_node("lorem", AttributeMap::new()).unwrap();

        assert_eq!(1, derived.node_count());
        assert!(derived.contains_node("existing"));
        assert!(!derived.contains_node("lorem"));
        assert!(log.lock().unwrap().is_empty());
        assert_eq!(with_plugins.state().identity(), derived.state().identity());
        assert!(Arc::ptr_eq(with_plugins.state(), derived.state()));
    }

    #[test]
    fn test_dispatch_veto() {
        let original = GraphRecord::new()
            .add_plugin("vetoer", VetoingPlugin)
            .unwrap();

        let result = original.add_node("lorem", AttributeMap::new());

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::PluginFailure { cause }
                if cause
                    .downcast_ref::<VetoError>()
                    .is_some_and(|veto_error| veto_error.label == "lorem")
        )));
        assert_eq!(0, original.node_count());
    }

    #[test]
    fn test_post_dispatch() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let graphrecord = GraphRecord::new()
            .add_plugin(
                "recorder",
                RecordingPlugin {
                    label: "recorder",
                    log: Arc::clone(&log),
                },
            )
            .unwrap();

        graphrecord
            .add_nodes(vec![
                ("lorem", AttributeMap::new()),
                ("ipsum", AttributeMap::new()),
            ])
            .unwrap();

        assert_eq!(
            vec![
                "recorder:on_add_nodes:len=2".to_string(),
                "recorder:post_add_nodes:delta=2".to_string(),
            ],
            *log.lock().unwrap()
        );

        let log = Arc::new(Mutex::new(Vec::new()));
        let graphrecord = GraphRecord::new()
            .add_plugin(
                "first",
                RecordingPlugin {
                    label: "first",
                    log: Arc::clone(&log),
                },
            )
            .unwrap()
            .add_plugin(
                "second",
                RecordingPlugin {
                    label: "second",
                    log: Arc::clone(&log),
                },
            )
            .unwrap();

        graphrecord.add_node("lorem", AttributeMap::new()).unwrap();

        assert_eq!(
            Some(&"first:post_add_nodes:delta=1".to_string()),
            log.lock().unwrap().get(2)
        );
        assert_eq!(
            Some(&"second:post_add_nodes:delta=1".to_string()),
            log.lock().unwrap().get(3)
        );

        let log = Arc::new(Mutex::new(Vec::new()));
        let graphrecord = GraphRecord::new()
            .add_plugin(
                "recorder",
                RecordingPlugin {
                    label: "recorder",
                    log: Arc::clone(&log),
                },
            )
            .unwrap();

        graphrecord
            .add_nodes(Vec::<(NodeIndex, AttributeMap)>::new())
            .unwrap();

        assert_eq!(
            Some(&"recorder:post_add_nodes:delta=0".to_string()),
            log.lock().unwrap().get(1)
        );
    }

    #[test]
    fn test_post_dispatch_post_veto() {
        let original = GraphRecord::new()
            .add_plugin("post_vetoer", PostVetoingPlugin)
            .unwrap();

        let result = original.add_node("lorem", AttributeMap::new());

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::PluginFailure { cause }
                if cause
                    .downcast_ref::<VetoError>()
                    .is_some_and(|veto_error| veto_error.label == "ipsum")
        )));
        assert_eq!(0, original.node_count());

        let log = Arc::new(Mutex::new(Vec::new()));
        let graphrecord = GraphRecord::new()
            .add_plugin(
                "recorder",
                RecordingPlugin {
                    label: "recorder",
                    log: Arc::clone(&log),
                },
            )
            .unwrap()
            .add_plugin("post_vetoer", PostVetoingPlugin)
            .unwrap();

        let result = graphrecord.add_node("lorem", AttributeMap::new());

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::PluginFailure { cause }
                if cause
                    .downcast_ref::<VetoError>()
                    .is_some_and(|veto_error| veto_error.label == "ipsum")
        )));
        assert!(
            log.lock()
                .unwrap()
                .contains(&"recorder:post_add_nodes:delta=1".to_string())
        );
    }

    #[test]
    fn test_post_dispatch_tracing() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let graphrecord = GraphRecord::new()
            .add_plugin(
                "tracer",
                TracingPlugin {
                    log: Arc::clone(&log),
                },
            )
            .unwrap();

        let graphrecord = graphrecord.add_group("dolor").unwrap();
        let graphrecord = graphrecord
            .add_nodes(vec![
                ("lorem", AttributeMap::new()),
                ("ipsum", AttributeMap::new()),
            ])
            .unwrap();
        let graphrecord = graphrecord
            .add_nodes_in_group(vec![("amet", AttributeMap::new())], "dolor")
            .unwrap();

        let graphrecord = graphrecord
            .add_edges(vec![("lorem", "ipsum", AttributeMap::new())])
            .unwrap();
        let first_edge_index = graphrecord.edge_indices().next().unwrap();
        let graphrecord = graphrecord
            .add_edges_in_group(vec![("amet", "lorem", AttributeMap::new())], "dolor")
            .unwrap();
        let second_edge_index = graphrecord
            .edge_indices()
            .find(|edge_index| *edge_index != first_edge_index)
            .unwrap();

        let graphrecord = graphrecord
            .add_nodes_to_group(vec!["ipsum"], "dolor")
            .unwrap();
        let graphrecord = graphrecord
            .add_edges_to_group(vec![first_edge_index], "dolor")
            .unwrap();

        let graphrecord = graphrecord
            .set_node_attributes(
                vec!["lorem"],
                AttributeMap::from([("sed".into(), true.into())]),
            )
            .unwrap();
        let graphrecord = graphrecord
            .replace_node_attributes(
                vec!["lorem"],
                AttributeMap::from([("amet".into(), 7.into())]),
            )
            .unwrap();
        let graphrecord = graphrecord
            .remove_node_attributes(vec!["lorem"], vec!["amet"])
            .unwrap();

        let graphrecord = graphrecord
            .set_edge_attributes(
                vec![first_edge_index],
                AttributeMap::from([("sed".into(), true.into())]),
            )
            .unwrap();
        let graphrecord = graphrecord
            .replace_edge_attributes(
                vec![first_edge_index],
                AttributeMap::from([("amet".into(), 7.into())]),
            )
            .unwrap();
        let graphrecord = graphrecord
            .remove_edge_attributes(vec![first_edge_index], vec!["amet"])
            .unwrap();

        let schema = Schema::infer(&graphrecord);
        let graphrecord = graphrecord.set_schema(schema).unwrap();
        let graphrecord = graphrecord.freeze_schema().unwrap();
        let graphrecord = graphrecord.unfreeze_schema().unwrap();

        let graphrecord = graphrecord
            .remove_nodes_from_group(vec!["ipsum"], "dolor")
            .unwrap();
        let graphrecord = graphrecord
            .remove_edges_from_group(vec![first_edge_index], "dolor")
            .unwrap();
        let graphrecord = graphrecord.remove_groups(vec!["dolor"]).unwrap();
        let graphrecord = graphrecord
            .remove_edges(vec![first_edge_index, second_edge_index])
            .unwrap();
        let graphrecord = graphrecord
            .remove_nodes(vec!["lorem", "ipsum", "amet"])
            .unwrap();
        let graphrecord = graphrecord.clear().unwrap();

        assert_eq!(0, graphrecord.node_count());
        assert_eq!(
            vec![
                "on_add_group".to_string(),
                "post_add_group".to_string(),
                "on_add_nodes".to_string(),
                "post_add_nodes".to_string(),
                "on_add_nodes_in_group".to_string(),
                "post_add_nodes_in_group".to_string(),
                "on_add_edges".to_string(),
                "post_add_edges".to_string(),
                "on_add_edges_in_group".to_string(),
                "post_add_edges_in_group".to_string(),
                "on_add_nodes_to_group".to_string(),
                "post_add_nodes_to_group".to_string(),
                "on_add_edges_to_group".to_string(),
                "post_add_edges_to_group".to_string(),
                "on_set_node_attributes".to_string(),
                "post_set_node_attributes".to_string(),
                "on_replace_node_attributes".to_string(),
                "post_replace_node_attributes".to_string(),
                "on_remove_node_attributes".to_string(),
                "post_remove_node_attributes".to_string(),
                "on_set_edge_attributes".to_string(),
                "post_set_edge_attributes".to_string(),
                "on_replace_edge_attributes".to_string(),
                "post_replace_edge_attributes".to_string(),
                "on_remove_edge_attributes".to_string(),
                "post_remove_edge_attributes".to_string(),
                "on_set_schema".to_string(),
                "post_set_schema".to_string(),
                "on_freeze_schema".to_string(),
                "post_freeze_schema".to_string(),
                "on_unfreeze_schema".to_string(),
                "post_unfreeze_schema".to_string(),
                "on_remove_nodes_from_group".to_string(),
                "post_remove_nodes_from_group".to_string(),
                "on_remove_edges_from_group".to_string(),
                "post_remove_edges_from_group".to_string(),
                "on_remove_groups".to_string(),
                "post_remove_groups".to_string(),
                "on_remove_edges".to_string(),
                "post_remove_edges".to_string(),
                "on_remove_nodes".to_string(),
                "post_remove_nodes".to_string(),
                "on_clear".to_string(),
                "post_clear".to_string(),
            ],
            *log.lock().unwrap()
        );
    }

    #[test]
    fn test_add_plugin() {
        let graphrecord = GraphRecord::new()
            .add_plugin("lorem", PassThroughPlugin)
            .unwrap();

        assert_eq!(
            vec![&PluginName::from("lorem")],
            graphrecord.plugins().collect::<Vec<_>>()
        );

        let initialized = GraphRecord::new()
            .add_plugin("ipsum", LifecyclePlugin)
            .unwrap();

        assert!(initialized.contains_group("dolor"));
    }

    #[test]
    fn test_invalid_add_plugin() {
        let graphrecord = GraphRecord::new()
            .add_plugin("lorem", PassThroughPlugin)
            .unwrap();

        let result = graphrecord.add_plugin("lorem", PassThroughPlugin);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::PluginAlreadyExists { name }
                if name == "lorem".into()
        )));
    }

    #[test]
    fn test_remove_plugin() {
        let graphrecord = GraphRecord::new()
            .add_plugin("lorem", PassThroughPlugin)
            .unwrap();

        let removed = graphrecord.remove_plugin("lorem").unwrap();

        assert_eq!(
            Vec::<&PluginName>::new(),
            removed.plugins().collect::<Vec<_>>()
        );

        let initialized = GraphRecord::new()
            .add_plugin("ipsum", LifecyclePlugin)
            .unwrap();

        let finalized = initialized.remove_plugin("ipsum").unwrap();

        assert!(!finalized.contains_group("dolor"));
        assert_eq!(
            Vec::<&PluginName>::new(),
            finalized.plugins().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_invalid_remove_plugin() {
        let result = GraphRecord::new().remove_plugin("lorem");

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::PluginNotFound { name }
                if name == "lorem".into()
        )));
    }

    #[test]
    fn test_plugins() {
        let graphrecord = GraphRecord::new()
            .add_plugin("lorem", PassThroughPlugin)
            .unwrap();

        let derived = graphrecord.add_node("ipsum", AttributeMap::new()).unwrap();

        assert_eq!(
            vec![&PluginName::from("lorem")],
            derived.plugins().collect::<Vec<_>>()
        );
    }
}
