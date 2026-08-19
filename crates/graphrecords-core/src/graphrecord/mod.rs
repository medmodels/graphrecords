pub mod batch;
pub mod changes;
pub mod datatypes;
#[cfg(feature = "io")]
mod io;
#[cfg(feature = "plugins")]
pub mod plugins;
pub mod schema;
pub(crate) mod state;
pub mod state_view;

#[cfg(feature = "plugins")]
pub use self::plugins::Plugin;
pub use self::{
    batch::{EdgeBatch, NodeBatch},
    changes::Changes,
    datatypes::{
        AttributeMap, AttributeName, AttributeNameView, EdgeIndex, Group, GroupView, Identifier,
        IdentifierView, NodeIndex, NodeIndexView, PluginName, PluginNameView, Value, ValueView,
    },
    state::{EdgeAddress, GroupAddress, NodeAddress, StateIdentity},
    state_view::{EdgeAttributeAddress, NodeAttributeAddress, StateView},
};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        changes::{
            AddEdges, AddEdgesInGroups, AddEdgesToGroup, AddGroup, AddNodes, AddNodesInGroups,
            AddNodesToGroup, Clear, FreezeSchema, RemoveEdgeAttributes, RemoveEdges,
            RemoveEdgesFromGroup, RemoveGroups, RemoveNodeAttributes, RemoveNodes,
            RemoveNodesFromGroup, ReplaceEdgeAttributes, ReplaceNodeAttributes, SetEdgeAttributes,
            SetNodeAttributes, SetSchema, UnfreezeSchema,
        },
        schema::Schema,
        state::GraphState,
    },
};
use graphrecords_utils::aliases::GrHashSet;
use std::sync::Arc;

#[derive(Clone)]
pub struct GraphRecord {
    state: Arc<GraphState>,
    #[cfg(feature = "plugins")]
    plugins: Arc<Vec<(PluginName, Arc<dyn Plugin>)>>,
}

impl GraphRecord {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(GraphState::new()),
            #[cfg(feature = "plugins")]
            plugins: Arc::new(Vec::new()),
        }
    }

    pub(crate) const fn state(&self) -> &Arc<GraphState> {
        &self.state
    }

    #[cfg(feature = "plugins")]
    pub(crate) fn apply(&self, changes: Changes) -> GraphRecordResult<Self> {
        let mut current = changes;

        for entry in self.plugins.iter() {
            let mut next = Changes::new();

            for change in current {
                next.extend(change.dispatch(entry.1.as_ref(), self)?);
            }

            current = next;

            if current.is_empty() {
                return Ok(Self {
                    state: Arc::clone(&self.state),
                    plugins: Arc::clone(&self.plugins),
                });
            }
        }

        let post_dispatch_hooks: Vec<_> = current
            .iter()
            .map(|change| change.post_dispatch_hook())
            .collect();

        let mut state = (*self.state).clone();

        for change in current {
            state = change.apply(state)?;
        }

        state.identity = StateIdentity::mint();

        let candidate = Self {
            state: Arc::new(state),
            plugins: Arc::clone(&self.plugins),
        };

        for hook in post_dispatch_hooks {
            for entry in self.plugins.iter() {
                hook(entry.1.as_ref(), self, &candidate)?;
            }
        }

        Ok(candidate)
    }

    #[cfg(not(feature = "plugins"))]
    pub(crate) fn apply(&self, changes: Changes) -> GraphRecordResult<Self> {
        let mut state = (*self.state).clone();

        for change in changes {
            state = change.apply(state)?;
        }

        state.identity = StateIdentity::mint();

        Ok(Self {
            state: Arc::new(state),
        })
    }

    #[cfg(feature = "plugins")]
    pub fn add_plugin(
        &self,
        name: PluginName,
        plugin: impl Plugin + 'static,
    ) -> GraphRecordResult<Self> {
        if self.plugins.iter().any(|entry| entry.0 == name) {
            return Err(GraphRecordError::PluginAlreadyExists { name });
        }

        let plugin: Arc<dyn Plugin> = Arc::new(plugin);
        let mut plugins = (*self.plugins).clone();
        plugins.push((name, Arc::clone(&plugin)));

        let candidate = Self {
            state: Arc::clone(&self.state),
            plugins: Arc::new(plugins),
        };

        let changes = plugin.initialize(&candidate)?;
        if changes.is_empty() {
            return Ok(candidate);
        }

        candidate.apply(changes)
    }

    #[cfg(feature = "plugins")]
    pub fn remove_plugin(&self, name: &PluginName) -> GraphRecordResult<Self> {
        let plugin = self
            .plugins
            .iter()
            .find(|entry| entry.0 == *name)
            .map(|entry| Arc::clone(&entry.1))
            .ok_or_else(|| GraphRecordError::PluginNotFound { name: name.clone() })?;

        let changes = plugin.finalize(self)?;
        let settled = if changes.is_empty() {
            Self {
                state: Arc::clone(&self.state),
                plugins: Arc::clone(&self.plugins),
            }
        } else {
            self.apply(changes)?
        };

        let plugins = settled
            .plugins
            .iter()
            .filter(|entry| entry.0 != *name)
            .cloned()
            .collect();

        Ok(Self {
            state: Arc::clone(&settled.state),
            plugins: Arc::new(plugins),
        })
    }

    #[cfg(feature = "plugins")]
    pub fn plugins(&self) -> impl Iterator<Item = &PluginName> {
        self.plugins.iter().map(|entry| &entry.0)
    }

    pub fn add_nodes(&self, batch: impl Into<NodeBatch>) -> GraphRecordResult<Self> {
        self.apply(AddNodes::new(batch.into()).into())
    }

    pub fn add_node(
        &self,
        node_index: NodeIndex,
        attributes: AttributeMap,
    ) -> GraphRecordResult<Self> {
        self.add_nodes(vec![(node_index, attributes)])
    }

    pub fn add_nodes_in_groups(
        &self,
        batch: impl Into<NodeBatch>,
        groups: Vec<Group>,
    ) -> GraphRecordResult<Self> {
        self.apply(AddNodesInGroups::new(batch.into(), groups).into())
    }

    pub fn add_node_in_group(
        &self,
        node_index: NodeIndex,
        attributes: AttributeMap,
        groups: Vec<Group>,
    ) -> GraphRecordResult<Self> {
        self.add_nodes_in_groups(vec![(node_index, attributes)], groups)
    }

    pub fn add_edges(&self, batch: impl Into<EdgeBatch>) -> GraphRecordResult<Self> {
        self.apply(AddEdges::new(batch.into()).into())
    }

    pub fn add_edge(
        &self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: AttributeMap,
    ) -> GraphRecordResult<Self> {
        self.add_edges(vec![(source_node_index, target_node_index, attributes)])
    }

    pub fn add_edges_in_groups(
        &self,
        batch: impl Into<EdgeBatch>,
        groups: Vec<Group>,
    ) -> GraphRecordResult<Self> {
        self.apply(AddEdgesInGroups::new(batch.into(), groups).into())
    }

    pub fn add_edge_in_group(
        &self,
        source_node_index: NodeIndex,
        target_node_index: NodeIndex,
        attributes: AttributeMap,
        groups: Vec<Group>,
    ) -> GraphRecordResult<Self> {
        self.add_edges_in_groups(
            vec![(source_node_index, target_node_index, attributes)],
            groups,
        )
    }

    pub fn remove_nodes(&self, node_indices: Vec<NodeIndex>) -> GraphRecordResult<Self> {
        self.apply(RemoveNodes::new(node_indices).into())
    }

    pub fn remove_edges(&self, edge_indices: Vec<EdgeIndex>) -> GraphRecordResult<Self> {
        self.apply(RemoveEdges::new(edge_indices).into())
    }

    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn keep_nodes(&self, node_indices: &[NodeIndex]) -> GraphRecordResult<Self> {
        let keep_addresses: GrHashSet<_> = node_indices
            .iter()
            .map(|node_index| {
                self.state.resolve_node_address(node_index).ok_or_else(|| {
                    GraphRecordError::NodeNotFound {
                        node_index: node_index.clone(),
                    }
                })
            })
            .collect::<GraphRecordResult<_>>()?;

        let nodes_to_remove = self
            .state
            .node_addresses()
            .filter(|address| !keep_addresses.contains(address))
            .map(|address| {
                NodeIndex::from(Identifier::from(
                    self.state.node_key(address).expect("Node must exist."),
                ))
            })
            .collect();

        self.remove_nodes(nodes_to_remove)
    }

    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn keep_edges(&self, edge_indices: &[EdgeIndex]) -> GraphRecordResult<Self> {
        let keep_addresses: GrHashSet<_> = edge_indices
            .iter()
            .map(|edge_index| {
                self.state
                    .resolve_edge_address(edge_index)
                    .ok_or(GraphRecordError::EdgeNotFound {
                        edge_index: *edge_index,
                    })
            })
            .collect::<GraphRecordResult<_>>()?;

        let edges_to_remove = self
            .state
            .edge_addresses()
            .filter(|address| !keep_addresses.contains(address))
            .map(|address| self.state.edge_index(address).expect("Edge must exist."))
            .collect();

        self.remove_edges(edges_to_remove)
    }

    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn keep_groups(&self, groups: &[Group]) -> GraphRecordResult<Self> {
        let keep_addresses: GrHashSet<_> = groups
            .iter()
            .map(|group| {
                self.state.resolve_group_address(group).ok_or_else(|| {
                    GraphRecordError::GroupNotFound {
                        group: group.clone(),
                    }
                })
            })
            .collect::<GraphRecordResult<_>>()?;

        let groups_to_remove = self
            .state
            .group_addresses()
            .filter(|address| !keep_addresses.contains(address))
            .map(|address| {
                self.state
                    .group_name(address)
                    .expect("Group must exist.")
                    .clone()
            })
            .collect();

        self.remove_groups(groups_to_remove)
    }

    pub fn set_node_attributes(
        &self,
        node_indices: Vec<NodeIndex>,
        attributes: AttributeMap,
    ) -> GraphRecordResult<Self> {
        self.apply(SetNodeAttributes::new(node_indices, attributes).into())
    }

    pub fn replace_node_attributes(
        &self,
        node_indices: Vec<NodeIndex>,
        attributes: AttributeMap,
    ) -> GraphRecordResult<Self> {
        self.apply(ReplaceNodeAttributes::new(node_indices, attributes).into())
    }

    pub fn remove_node_attributes(
        &self,
        node_indices: Vec<NodeIndex>,
        attribute_names: Vec<AttributeName>,
    ) -> GraphRecordResult<Self> {
        self.apply(RemoveNodeAttributes::new(node_indices, attribute_names).into())
    }

    pub fn set_edge_attributes(
        &self,
        edge_indices: Vec<EdgeIndex>,
        attributes: AttributeMap,
    ) -> GraphRecordResult<Self> {
        self.apply(SetEdgeAttributes::new(edge_indices, attributes).into())
    }

    pub fn replace_edge_attributes(
        &self,
        edge_indices: Vec<EdgeIndex>,
        attributes: AttributeMap,
    ) -> GraphRecordResult<Self> {
        self.apply(ReplaceEdgeAttributes::new(edge_indices, attributes).into())
    }

    pub fn remove_edge_attributes(
        &self,
        edge_indices: Vec<EdgeIndex>,
        attribute_names: Vec<AttributeName>,
    ) -> GraphRecordResult<Self> {
        self.apply(RemoveEdgeAttributes::new(edge_indices, attribute_names).into())
    }

    pub fn add_group(&self, group: Group) -> GraphRecordResult<Self> {
        self.apply(AddGroup::new(group).into())
    }

    pub fn remove_groups(&self, groups: Vec<Group>) -> GraphRecordResult<Self> {
        self.apply(RemoveGroups::new(groups).into())
    }

    pub fn add_nodes_to_group(
        &self,
        group: Group,
        node_indices: Vec<NodeIndex>,
    ) -> GraphRecordResult<Self> {
        self.apply(AddNodesToGroup::new(group, node_indices).into())
    }

    pub fn remove_nodes_from_group(
        &self,
        group: Group,
        node_indices: Vec<NodeIndex>,
    ) -> GraphRecordResult<Self> {
        self.apply(RemoveNodesFromGroup::new(group, node_indices).into())
    }

    pub fn add_edges_to_group(
        &self,
        group: Group,
        edge_indices: Vec<EdgeIndex>,
    ) -> GraphRecordResult<Self> {
        self.apply(AddEdgesToGroup::new(group, edge_indices).into())
    }

    pub fn remove_edges_from_group(
        &self,
        group: Group,
        edge_indices: Vec<EdgeIndex>,
    ) -> GraphRecordResult<Self> {
        self.apply(RemoveEdgesFromGroup::new(group, edge_indices).into())
    }

    pub fn set_schema(&self, schema: Schema) -> GraphRecordResult<Self> {
        self.apply(SetSchema::new(schema).into())
    }

    pub fn freeze_schema(&self) -> GraphRecordResult<Self> {
        self.apply(FreezeSchema::new().into())
    }

    pub fn unfreeze_schema(&self) -> GraphRecordResult<Self> {
        self.apply(UnfreezeSchema::new().into())
    }

    pub fn clear(&self) -> GraphRecordResult<Self> {
        self.apply(Clear::new().into())
    }

    #[must_use]
    pub fn compact(&self) -> Self {
        let mut state = (*self.state).clone();
        state.compact();
        state.identity = StateIdentity::mint();

        Self {
            state: Arc::new(state),
            #[cfg(feature = "plugins")]
            plugins: Arc::clone(&self.plugins),
        }
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.state.node_count()
    }

    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.state.edge_count()
    }

    #[must_use]
    pub fn group_count(&self) -> usize {
        self.state.group_count()
    }

    #[must_use]
    pub fn contains_node(&self, node_index: &NodeIndex) -> bool {
        self.state.resolve_node_address(node_index).is_some()
    }

    #[must_use]
    pub fn contains_edge(&self, edge_index: &EdgeIndex) -> bool {
        self.state.resolve_edge_address(edge_index).is_some()
    }

    #[must_use]
    pub fn contains_group(&self, group: &Group) -> bool {
        self.state.resolve_group_address(group).is_some()
    }

    #[must_use]
    pub fn schema(&self) -> &Schema {
        self.state.schema()
    }

    #[must_use]
    pub fn node_attribute(
        &self,
        node_index: &NodeIndex,
        attribute_name: &AttributeName,
    ) -> Option<ValueView<'_>> {
        let address = self.state.resolve_node_address(node_index)?;

        self.state.node_attribute_by_name(address, attribute_name)
    }

    #[must_use]
    pub fn edge_attribute(
        &self,
        edge_index: &EdgeIndex,
        attribute_name: &AttributeName,
    ) -> Option<ValueView<'_>> {
        let address = self.state.resolve_edge_address(edge_index)?;

        self.state.edge_attribute_by_name(address, attribute_name)
    }

    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn node_indices(&self) -> impl Iterator<Item = NodeIndexView<'_>> {
        self.state.node_addresses().map(|address| {
            self.state
                .node_key(address)
                .map(NodeIndexView::from)
                .expect("Node must exist.")
        })
    }

    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn edge_indices(&self) -> impl Iterator<Item = EdgeIndex> + '_ {
        self.state
            .edge_addresses()
            .map(|address| self.state.edge_index(address).expect("Edge must exist."))
    }

    #[must_use]
    pub fn edge_endpoints(
        &self,
        edge_index: &EdgeIndex,
    ) -> Option<(NodeIndexView<'_>, NodeIndexView<'_>)> {
        let address = self.state.resolve_edge_address(edge_index)?;
        let endpoints = self.state.edge_endpoints(address)?;

        let source = self.state.node_key(endpoints.source_address)?;
        let target = self.state.node_key(endpoints.target_address)?;

        Some((NodeIndexView::from(source), NodeIndexView::from(target)))
    }
}

impl Default for GraphRecord {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::{AttributeMap, GraphRecord, Identifier, NodeIndexView};
    use crate::{
        errors::GraphRecordError,
        graphrecord::{
            EdgeIndex, Value,
            datatypes::DataType,
            schema::{AttributeDataType, AttributeType, GroupSchema, Schema, SchemaType},
        },
    };
    use std::{collections::HashMap, sync::Arc};

    fn create_lorem_attributes() -> AttributeMap {
        AttributeMap::from([("lorem".into(), 42.into())])
    }

    fn create_graphrecord_with_two_nodes() -> GraphRecord {
        GraphRecord::new()
            .add_node("lorem".into(), create_lorem_attributes())
            .unwrap()
            .add_node("ipsum".into(), AttributeMap::new())
            .unwrap()
    }

    fn create_graphrecord_with_one_edge() -> (GraphRecord, EdgeIndex) {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_edge("lorem".into(), "ipsum".into(), create_lorem_attributes())
            .unwrap();
        let edge_index = graphrecord.edge_indices().next().unwrap();

        (graphrecord, edge_index)
    }

    #[test]
    fn test_new() {
        let graphrecord = GraphRecord::new();

        assert_eq!(0, graphrecord.node_count());
        assert_eq!(0, graphrecord.edge_count());
        assert_eq!(0, graphrecord.group_count());
    }

    #[test]
    fn test_add_nodes() {
        let graphrecord = create_graphrecord_with_two_nodes();

        assert_eq!(2, graphrecord.node_count());
        assert!(graphrecord.contains_node(&"lorem".into()));
        assert!(graphrecord.contains_node(&"ipsum".into()));
        assert_eq!(
            Some(42.into()),
            graphrecord
                .node_attribute(&"lorem".into(), &"lorem".into())
                .map(Value::from)
        );

        let derived = graphrecord
            .add_node("dolor".into(), AttributeMap::new())
            .unwrap();

        assert_eq!(2, graphrecord.node_count());
        assert!(!graphrecord.contains_node(&"dolor".into()));
        assert_eq!(3, derived.node_count());
        assert!(!Arc::ptr_eq(graphrecord.state(), derived.state()));
    }

    #[test]
    fn test_invalid_add_nodes() {
        let original = create_graphrecord_with_two_nodes();

        let result = original
            .add_nodes(vec![
                ("dolor".into(), AttributeMap::new()),
                ("lorem".into(), AttributeMap::new()),
            ])
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::NodeAlreadyExists {
                node_index: "lorem".into()
            }),
            result
        );
        assert_eq!(2, original.node_count());
        assert!(!original.contains_node(&"dolor".into()));
    }

    #[test]
    fn test_add_nodes_in_groups() {
        let graphrecord = GraphRecord::new()
            .add_group("dolor".into())
            .unwrap()
            .add_nodes_in_groups(
                vec![("lorem".into(), AttributeMap::new())],
                vec!["dolor".into()],
            )
            .unwrap();

        assert!(graphrecord.contains_node(&"lorem".into()));

        let state = graphrecord.state();
        let node_address = state.resolve_node_address(&"lorem".into()).unwrap();
        let group_address = state.resolve_group_address(&"dolor".into()).unwrap();

        assert!(
            state
                .node_memberships(node_address)
                .any(|membership| membership == group_address)
        );
        assert!(
            graphrecord
                .add_nodes_to_group("dolor".into(), vec!["lorem".into()])
                .is_err_and(|error| matches!(error, GraphRecordError::NodeAlreadyInGroup { .. }))
        );
    }

    #[test]
    fn test_invalid_add_nodes_in_groups() {
        let result = GraphRecord::new()
            .add_nodes_in_groups(
                vec![("lorem".into(), AttributeMap::new())],
                vec!["dolor".into()],
            )
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::GroupNotFound {
                group: "dolor".into()
            }),
            result
        );
    }

    #[test]
    fn test_add_edges() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        assert_eq!(1, graphrecord.edge_count());
        assert!(graphrecord.contains_edge(&edge_index));
        assert_eq!(
            Some(42.into()),
            graphrecord
                .edge_attribute(&edge_index, &"lorem".into())
                .map(Value::from)
        );

        let graphrecord = create_graphrecord_with_two_nodes()
            .add_edges(vec![
                ("lorem".into(), "ipsum".into(), create_lorem_attributes()),
                ("ipsum".into(), "lorem".into(), AttributeMap::new()),
                ("lorem".into(), "lorem".into(), AttributeMap::new()),
            ])
            .unwrap();

        let edge_indices: Vec<_> = graphrecord.edge_indices().collect();

        assert_eq!(3, graphrecord.edge_count());
        assert_eq!(3, edge_indices.len());
        assert!(
            edge_indices
                .iter()
                .all(|edge_index| graphrecord.contains_edge(edge_index))
        );
    }

    #[test]
    fn test_invalid_add_edges() {
        let result = create_graphrecord_with_two_nodes()
            .add_edge("lorem".into(), "dolor".into(), AttributeMap::new())
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::NodeNotFound {
                node_index: "dolor".into()
            }),
            result
        );

        let original = create_graphrecord_with_two_nodes();

        let result = original
            .add_edges(vec![
                ("lorem".into(), "ipsum".into(), AttributeMap::new()),
                ("lorem".into(), "dolor".into(), AttributeMap::new()),
            ])
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::NodeNotFound {
                node_index: "dolor".into()
            }),
            result
        );
        assert_eq!(0, original.edge_count());
    }

    #[test]
    fn test_add_edges_in_groups() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor".into())
            .unwrap()
            .add_edges_in_groups(
                vec![("lorem".into(), "ipsum".into(), AttributeMap::new())],
                vec!["dolor".into()],
            )
            .unwrap();
        let edge_index = graphrecord.edge_indices().next().unwrap();

        let state = graphrecord.state();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let group_address = state.resolve_group_address(&"dolor".into()).unwrap();

        assert!(
            state
                .edge_memberships(edge_address)
                .any(|membership| membership == group_address)
        );
        assert!(
            graphrecord
                .add_edges_to_group("dolor".into(), vec![edge_index])
                .is_err_and(|error| matches!(error, GraphRecordError::EdgeAlreadyInGroup { .. }))
        );
    }

    #[test]
    fn test_invalid_add_edges_in_groups() {
        let result = create_graphrecord_with_two_nodes()
            .add_edges_in_groups(
                vec![("lorem".into(), "ipsum".into(), AttributeMap::new())],
                vec!["dolor".into()],
            )
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::GroupNotFound {
                group: "dolor".into()
            }),
            result
        );
    }

    #[test]
    fn test_remove_nodes() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let removed = graphrecord.remove_nodes(vec!["lorem".into()]).unwrap();

        assert_eq!(1, removed.node_count());
        assert!(!removed.contains_node(&"lorem".into()));
        assert_eq!(0, removed.edge_count());
        assert!(!removed.contains_edge(&edge_index));
        assert_eq!(2, graphrecord.node_count());

        let graphrecord = create_graphrecord_with_two_nodes()
            .remove_nodes(vec!["lorem".into()])
            .unwrap()
            .add_node(
                "lorem".into(),
                AttributeMap::from([("sed".into(), 7.into())]),
            )
            .unwrap();

        assert_eq!(2, graphrecord.node_count());
        assert_eq!(
            Some(7.into()),
            graphrecord
                .node_attribute(&"lorem".into(), &"sed".into())
                .map(Value::from)
        );
    }

    #[test]
    fn test_invalid_remove_nodes() {
        let result = create_graphrecord_with_two_nodes()
            .remove_nodes(vec!["dolor".into()])
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::NodeNotFound {
                node_index: "dolor".into()
            }),
            result
        );
    }

    #[test]
    fn test_remove_edges() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let removed = graphrecord.remove_edges(vec![edge_index]).unwrap();

        assert_eq!(0, removed.edge_count());
        assert!(!removed.contains_edge(&edge_index));
        assert_eq!(2, removed.node_count());
    }

    #[test]
    fn test_invalid_remove_edges() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let removed = graphrecord.remove_edges(vec![edge_index]).unwrap();

        let result = removed.remove_edges(vec![edge_index]).map(|_| ());

        assert_eq!(Err(GraphRecordError::EdgeNotFound { edge_index }), result);
    }

    #[test]
    fn test_keep_nodes() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_node("dolor".into(), AttributeMap::new())
            .unwrap();

        let kept = graphrecord.keep_nodes(&["lorem".into()]).unwrap();

        assert_eq!(1, kept.node_count());
        assert!(kept.contains_node(&"lorem".into()));
        assert!(!kept.contains_node(&"ipsum".into()));

        let kept = create_graphrecord_with_two_nodes().keep_nodes(&[]).unwrap();

        assert_eq!(0, kept.node_count());
    }

    #[test]
    fn test_invalid_keep_nodes() {
        let result = create_graphrecord_with_two_nodes()
            .keep_nodes(&["dolor".into()])
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::NodeNotFound {
                node_index: "dolor".into()
            }),
            result
        );
    }

    #[test]
    fn test_keep_edges() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_node("dolor".into(), AttributeMap::new())
            .unwrap()
            .add_edge("lorem".into(), "ipsum".into(), AttributeMap::new())
            .unwrap()
            .add_edge("ipsum".into(), "dolor".into(), AttributeMap::new())
            .unwrap();
        let first_edge_index = graphrecord.edge_indices().next().unwrap();

        let kept = graphrecord.keep_edges(&[first_edge_index]).unwrap();

        assert_eq!(1, kept.edge_count());
        assert!(kept.contains_edge(&first_edge_index));

        let (graphrecord, _) = create_graphrecord_with_one_edge();

        let kept = graphrecord.keep_edges(&[]).unwrap();

        assert_eq!(0, kept.edge_count());
        assert_eq!(2, kept.node_count());
    }

    #[test]
    fn test_invalid_keep_edges() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let removed = graphrecord.remove_edges(vec![edge_index]).unwrap();

        let result = removed.keep_edges(&[edge_index]).map(|_| ());

        assert_eq!(Err(GraphRecordError::EdgeNotFound { edge_index }), result);
    }

    #[test]
    fn test_keep_groups() {
        let graphrecord = GraphRecord::new()
            .add_group("lorem".into())
            .unwrap()
            .add_group("ipsum".into())
            .unwrap();

        let kept = graphrecord.keep_groups(&["lorem".into()]).unwrap();

        assert_eq!(1, kept.group_count());
        assert!(kept.contains_group(&"lorem".into()));
        assert!(!kept.contains_group(&"ipsum".into()));

        let graphrecord = GraphRecord::new().add_group("lorem".into()).unwrap();

        let kept = graphrecord.keep_groups(&[]).unwrap();

        assert_eq!(0, kept.group_count());
    }

    #[test]
    fn test_invalid_keep_groups() {
        let result = GraphRecord::new()
            .keep_groups(&["lorem".into()])
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::GroupNotFound {
                group: "lorem".into()
            }),
            result
        );
    }

    #[test]
    fn test_set_node_attributes() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .set_node_attributes(
                vec!["ipsum".into()],
                AttributeMap::from([("sed".into(), true.into())]),
            )
            .unwrap();

        assert_eq!(
            Some(true.into()),
            graphrecord
                .node_attribute(&"ipsum".into(), &"sed".into())
                .map(Value::from)
        );
    }

    #[test]
    fn test_replace_node_attributes() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .replace_node_attributes(vec!["lorem".into()], AttributeMap::new())
            .unwrap();

        assert_eq!(
            None,
            graphrecord.node_attribute(&"lorem".into(), &"lorem".into())
        );
    }

    #[test]
    fn test_remove_node_attributes() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .remove_node_attributes(vec!["lorem".into()], vec!["lorem".into()])
            .unwrap();

        assert_eq!(
            None,
            graphrecord.node_attribute(&"lorem".into(), &"lorem".into())
        );
    }

    #[test]
    fn test_invalid_remove_node_attributes() {
        let result = create_graphrecord_with_two_nodes()
            .remove_node_attributes(vec!["ipsum".into()], vec!["sed".into()])
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::NodeAttributeNotFound {
                node_index: "ipsum".into(),
                attribute_name: "sed".into(),
            }),
            result
        );
    }

    #[test]
    fn test_set_edge_attributes() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .set_edge_attributes(
                vec![edge_index],
                AttributeMap::from([("sed".into(), true.into())]),
            )
            .unwrap();

        assert_eq!(
            Some(true.into()),
            graphrecord
                .edge_attribute(&edge_index, &"sed".into())
                .map(Value::from)
        );
    }

    #[test]
    fn test_replace_edge_attributes() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .replace_edge_attributes(vec![edge_index], AttributeMap::new())
            .unwrap();

        assert_eq!(
            None,
            graphrecord.edge_attribute(&edge_index, &"lorem".into())
        );
    }

    #[test]
    fn test_remove_edge_attributes() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .remove_edge_attributes(vec![edge_index], vec!["lorem".into()])
            .unwrap();

        assert_eq!(
            None,
            graphrecord.edge_attribute(&edge_index, &"lorem".into())
        );
    }

    #[test]
    fn test_add_group() {
        let graphrecord = GraphRecord::new().add_group("lorem".into()).unwrap();

        assert_eq!(1, graphrecord.group_count());
        assert!(graphrecord.contains_group(&"lorem".into()));
    }

    #[test]
    fn test_invalid_add_group() {
        let result = GraphRecord::new()
            .add_group("lorem".into())
            .unwrap()
            .add_group("lorem".into())
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::GroupAlreadyExists {
                group: "lorem".into()
            }),
            result
        );
    }

    #[test]
    fn test_remove_groups() {
        let graphrecord = GraphRecord::new()
            .add_group("lorem".into())
            .unwrap()
            .remove_groups(vec!["lorem".into()])
            .unwrap();

        assert_eq!(0, graphrecord.group_count());
    }

    #[test]
    fn test_invalid_remove_groups() {
        let result = GraphRecord::new()
            .remove_groups(vec!["lorem".into()])
            .map(|_| ());

        assert_eq!(
            Err(GraphRecordError::GroupNotFound {
                group: "lorem".into()
            }),
            result
        );
    }

    #[test]
    fn test_add_nodes_to_group() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor".into())
            .unwrap()
            .add_nodes_to_group("dolor".into(), vec!["lorem".into()])
            .unwrap();

        let state = graphrecord.state();
        let node_address = state.resolve_node_address(&"lorem".into()).unwrap();
        let group_address = state.resolve_group_address(&"dolor".into()).unwrap();

        assert!(
            state
                .node_memberships(node_address)
                .any(|membership| membership == group_address)
        );
    }

    #[test]
    fn test_invalid_add_nodes_to_group() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor".into())
            .unwrap()
            .add_nodes_to_group("dolor".into(), vec!["lorem".into()])
            .unwrap();

        assert!(
            graphrecord
                .add_nodes_to_group("dolor".into(), vec!["lorem".into()])
                .is_err_and(|error| matches!(error, GraphRecordError::NodeAlreadyInGroup { .. }))
        );
    }

    #[test]
    fn test_remove_nodes_from_group() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor".into())
            .unwrap()
            .add_nodes_to_group("dolor".into(), vec!["lorem".into()])
            .unwrap()
            .remove_nodes_from_group("dolor".into(), vec!["lorem".into()])
            .unwrap();

        let state = graphrecord.state();
        let node_address = state.resolve_node_address(&"lorem".into()).unwrap();
        let group_address = state.resolve_group_address(&"dolor".into()).unwrap();

        assert!(
            !state
                .node_memberships(node_address)
                .any(|membership| membership == group_address)
        );
    }

    #[test]
    fn test_invalid_remove_nodes_from_group() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor".into())
            .unwrap()
            .add_nodes_to_group("dolor".into(), vec!["lorem".into()])
            .unwrap()
            .remove_nodes_from_group("dolor".into(), vec!["lorem".into()])
            .unwrap();

        assert!(
            graphrecord
                .remove_nodes_from_group("dolor".into(), vec!["lorem".into()])
                .is_err_and(|error| matches!(error, GraphRecordError::NodeNotInGroup { .. }))
        );
    }

    #[test]
    fn test_add_edges_to_group() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .add_group("dolor".into())
            .unwrap()
            .add_edges_to_group("dolor".into(), vec![edge_index])
            .unwrap();

        let state = graphrecord.state();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let group_address = state.resolve_group_address(&"dolor".into()).unwrap();

        assert!(
            state
                .edge_memberships(edge_address)
                .any(|membership| membership == group_address)
        );
    }

    #[test]
    fn test_invalid_add_edges_to_group() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .add_group("dolor".into())
            .unwrap()
            .add_edges_to_group("dolor".into(), vec![edge_index])
            .unwrap();

        assert!(
            graphrecord
                .add_edges_to_group("dolor".into(), vec![edge_index])
                .is_err_and(|error| matches!(error, GraphRecordError::EdgeAlreadyInGroup { .. }))
        );
    }

    #[test]
    fn test_remove_edges_from_group() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .add_group("dolor".into())
            .unwrap()
            .add_edges_to_group("dolor".into(), vec![edge_index])
            .unwrap()
            .remove_edges_from_group("dolor".into(), vec![edge_index])
            .unwrap();

        let state = graphrecord.state();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let group_address = state.resolve_group_address(&"dolor".into()).unwrap();

        assert!(
            !state
                .edge_memberships(edge_address)
                .any(|membership| membership == group_address)
        );
    }

    #[test]
    fn test_invalid_remove_edges_from_group() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .add_group("dolor".into())
            .unwrap()
            .add_edges_to_group("dolor".into(), vec![edge_index])
            .unwrap()
            .remove_edges_from_group("dolor".into(), vec![edge_index])
            .unwrap();

        assert!(
            graphrecord
                .remove_edges_from_group("dolor".into(), vec![edge_index])
                .is_err_and(|error| matches!(error, GraphRecordError::EdgeNotInGroup { .. }))
        );
    }

    #[test]
    fn test_set_schema() {
        let group_schema = GroupSchema::new(
            HashMap::from([(
                "lorem".into(),
                AttributeDataType::new(DataType::Int, AttributeType::Continuous).unwrap(),
            )])
            .into(),
            HashMap::new().into(),
        );
        let schema = Schema::new_provided(HashMap::new(), group_schema);

        let graphrecord = create_graphrecord_with_two_nodes()
            .set_node_attributes(vec!["ipsum".into()], create_lorem_attributes())
            .unwrap()
            .set_schema(schema.clone())
            .unwrap();

        assert_eq!(&schema, graphrecord.schema());
    }

    #[test]
    fn test_invalid_set_schema() {
        let group_schema = GroupSchema::new(
            HashMap::from([(
                "sed".into(),
                AttributeDataType::new(DataType::Int, AttributeType::Continuous).unwrap(),
            )])
            .into(),
            HashMap::new().into(),
        );
        let schema = Schema::new_provided(HashMap::new(), group_schema);

        let result = create_graphrecord_with_two_nodes().set_schema(schema);

        assert!(result.is_err());
    }

    #[test]
    fn test_freeze_schema() {
        let graphrecord = create_graphrecord_with_two_nodes();

        let frozen = graphrecord.freeze_schema().unwrap();

        assert_eq!(&SchemaType::Provided, frozen.schema().schema_type());
    }

    #[test]
    fn test_unfreeze_schema() {
        let frozen = create_graphrecord_with_two_nodes().freeze_schema().unwrap();

        let unfrozen = frozen.unfreeze_schema().unwrap();

        assert_eq!(&SchemaType::Inferred, unfrozen.schema().schema_type());
    }

    #[test]
    fn test_clear() {
        let (graphrecord, _) = create_graphrecord_with_one_edge();

        let cleared = graphrecord.clear().unwrap();

        assert_eq!(0, cleared.node_count());
        assert_eq!(0, cleared.edge_count());
        assert_eq!(2, graphrecord.node_count());
    }

    #[test]
    fn test_compact() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let removable = graphrecord
            .add_node("dolor".into(), AttributeMap::new())
            .unwrap()
            .remove_nodes(vec!["dolor".into()])
            .unwrap();

        let compacted = removable.compact();

        assert_eq!(2, compacted.node_count());
        assert_eq!(1, compacted.edge_count());
        assert!(!compacted.contains_edge(&edge_index));
        assert_eq!(
            Some(42.into()),
            compacted
                .node_attribute(&"lorem".into(), &"lorem".into())
                .map(Value::from)
        );
    }

    #[test]
    fn test_node_count() {
        assert_eq!(2, create_graphrecord_with_two_nodes().node_count());
    }

    #[test]
    fn test_edge_count() {
        let (graphrecord, _) = create_graphrecord_with_one_edge();

        assert_eq!(1, graphrecord.edge_count());
    }

    #[test]
    fn test_group_count() {
        let graphrecord = GraphRecord::new().add_group("lorem".into()).unwrap();

        assert_eq!(1, graphrecord.group_count());
    }

    #[test]
    fn test_contains_node() {
        let graphrecord = create_graphrecord_with_two_nodes();

        assert!(graphrecord.contains_node(&"lorem".into()));
        assert!(!graphrecord.contains_node(&"dolor".into()));
    }

    #[test]
    fn test_contains_edge() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        assert!(graphrecord.contains_edge(&edge_index));
        assert!(!graphrecord.contains_edge(&EdgeIndex::new(
            edge_index.tag().wrapping_add(1),
            edge_index.offset()
        )));
    }

    #[test]
    fn test_contains_group() {
        let graphrecord = GraphRecord::new().add_group("lorem".into()).unwrap();

        assert!(graphrecord.contains_group(&"lorem".into()));
        assert!(!graphrecord.contains_group(&"ipsum".into()));
    }

    #[test]
    fn test_node_attribute() {
        let graphrecord = create_graphrecord_with_two_nodes();

        assert_eq!(
            Some(42.into()),
            graphrecord
                .node_attribute(&"lorem".into(), &"lorem".into())
                .map(Value::from)
        );
    }

    #[test]
    fn test_edge_attribute() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        assert_eq!(
            Some(42.into()),
            graphrecord
                .edge_attribute(&edge_index, &"lorem".into())
                .map(Value::from)
        );
    }

    #[test]
    fn test_edge_endpoints() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        assert_eq!(
            Some((
                NodeIndexView::from(&Identifier::from("lorem")),
                NodeIndexView::from(&Identifier::from("ipsum"))
            )),
            graphrecord.edge_endpoints(&edge_index)
        );
    }

    #[test]
    fn test_clone() {
        let graphrecord = create_graphrecord_with_two_nodes();
        let cloned = graphrecord.clone();

        assert!(Arc::ptr_eq(graphrecord.state(), cloned.state()));
    }

    #[test]
    fn test_default() {
        assert_eq!(0, GraphRecord::default().node_count());
    }
}
