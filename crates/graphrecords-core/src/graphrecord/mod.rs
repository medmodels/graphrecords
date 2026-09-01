#[cfg(feature = "arrow")]
mod arrow;
pub mod batch;
pub mod changes;
pub mod datatypes;
#[cfg(any(feature = "polars", feature = "arrow"))]
mod frame;
#[cfg(feature = "io")]
mod io;
#[cfg(feature = "plugins")]
pub mod plugins;
#[cfg(feature = "polars")]
mod polars;
pub mod schema;
pub mod selection;
#[cfg(feature = "serde")]
mod serde;
pub mod source;
pub(crate) mod state;
pub mod state_view;
pub mod views;
pub mod writer;

#[cfg(feature = "arrow")]
pub use self::arrow::ArrowTables;
#[cfg(any(feature = "polars", feature = "arrow"))]
pub use self::frame::{Export, Tables};
#[cfg(feature = "io")]
pub use self::io::RonFile;
#[cfg(feature = "plugins")]
pub use self::plugins::Plugin;
#[cfg(feature = "polars")]
pub use self::polars::PolarsFrames;
pub use self::{
    batch::{EdgeBatch, NodeBatch},
    changes::Changes,
    datatypes::{
        AttributeMap, AttributeName, AttributeNameView, EdgeDirection, EdgeIndex, GroupIndex,
        GroupIndexView, Identifier, IdentifierView, NodeIndex, NodeIndexView, OnConflict,
        PluginName, PluginNameView, Value, ValueView,
    },
    selection::{EntityDomain, MultipleSelection, SingleSelection},
    source::{EdgeSource, NodeSource},
    state::{EdgeAddress, GroupAddress, NodeAddress, StateIdentity},
    state_view::{EdgeAttributeAddress, NodeAttributeAddress, StateView},
    views::{ConnectingEdges, EdgeView, GroupView, NodeView},
    writer::Writer,
};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        changes::{
            AddEdges, AddEdgesInGroup, AddEdgesToGroup, AddGroup, AddNodes, AddNodesInGroup,
            AddNodesToGroup, Clear, FreezeSchema, RemoveEdgeAttributes, RemoveEdges,
            RemoveEdgesFromGroup, RemoveGroups, RemoveNodeAttributes, RemoveNodes,
            RemoveNodesFromGroup, ReplaceEdgeAttributes, ReplaceNodeAttributes, SetEdgeAttributes,
            SetNodeAttributes, SetSchema, UnfreezeSchema,
        },
        datatypes::collect_attributes,
        schema::Schema,
        state::GraphState,
    },
};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::{fmt, sync::Arc};

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

    #[must_use]
    pub fn with_schema(schema: Schema) -> Self {
        let mut state = GraphState::new();
        state.replace_schema(Arc::new(schema));

        Self {
            state: Arc::new(state),
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
                next.extend(change.pre_dispatch(entry.1.as_ref(), self)?);
            }

            current = next;

            if current.is_empty() {
                return Ok(Self {
                    state: Arc::clone(&self.state),
                    plugins: Arc::clone(&self.plugins),
                });
            }
        }

        let mut state = (*self.state).clone();

        for change in current.iter() {
            state = change.apply(state)?;
        }

        state.identity = StateIdentity::mint();

        let candidate = Self {
            state: Arc::new(state),
            plugins: Arc::clone(&self.plugins),
        };

        for change in current.iter() {
            for entry in self.plugins.iter() {
                change.post_dispatch(entry.1.as_ref(), self, &candidate)?;
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
        name: impl Into<PluginName>,
        plugin: impl Plugin + 'static,
    ) -> GraphRecordResult<Self> {
        self.add_plugin_entry(name.into(), Arc::new(plugin))
    }

    #[cfg(feature = "plugins")]
    fn add_plugin_entry(
        &self,
        name: PluginName,
        plugin: Arc<dyn Plugin>,
    ) -> GraphRecordResult<Self> {
        if self.plugins.iter().any(|entry| entry.0 == name) {
            return Err(GraphRecordError::PluginAlreadyExists { name });
        }

        let initializer = Arc::clone(&plugin);
        let mut plugins = (*self.plugins).clone();
        plugins.push((name, plugin));

        let candidate = Self {
            state: Arc::clone(&self.state),
            plugins: Arc::new(plugins),
        };

        let changes = initializer.initialize(&candidate)?;
        if changes.is_empty() {
            return Ok(candidate);
        }

        candidate.apply(changes)
    }

    #[cfg(feature = "plugins")]
    pub fn remove_plugin(&self, name: impl Into<PluginName>) -> GraphRecordResult<Self> {
        let name = name.into();

        let plugin = self
            .plugins
            .iter()
            .find(|entry| entry.0 == name)
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
            .filter(|entry| entry.0 != name)
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

    #[cfg(feature = "plugins")]
    pub fn plugin_entries(&self) -> impl Iterator<Item = (&PluginName, &Arc<dyn Plugin>)> {
        self.plugins.iter().map(|entry| (&entry.0, &entry.1))
    }

    #[cfg(feature = "plugins")]
    pub fn with_plugins(
        &self,
        plugins: impl IntoIterator<Item = (impl Into<PluginName>, Arc<dyn Plugin>)>,
    ) -> GraphRecordResult<Self> {
        let mut record = self.clone();

        for (name, plugin) in plugins {
            record = record.add_plugin_entry(name.into(), plugin)?;
        }

        Ok(record)
    }

    #[cfg(feature = "plugins")]
    pub fn reattach_plugins(
        &self,
        plugins: impl IntoIterator<Item = (impl Into<PluginName>, Arc<dyn Plugin>)>,
    ) -> GraphRecordResult<Self> {
        let mut entries: Vec<(PluginName, Arc<dyn Plugin>)> = Vec::new();

        for (name, plugin) in plugins {
            let name = name.into();

            if entries.iter().any(|entry| entry.0 == name) {
                return Err(GraphRecordError::PluginAlreadyExists { name });
            }

            entries.push((name, plugin));
        }

        Ok(Self {
            state: Arc::clone(&self.state),
            plugins: Arc::new(entries),
        })
    }

    pub fn add_nodes(&self, source: impl NodeSource) -> GraphRecordResult<Self> {
        self.apply(AddNodes::new(source.collect_nodes()?).into())
    }

    pub fn add_node(
        &self,
        node_index: impl SingleSelection<NodeIndex>,
        attributes: impl IntoIterator<Item = (impl Into<AttributeName>, impl Into<Value>)>,
    ) -> GraphRecordResult<Self> {
        let attributes = collect_attributes(attributes);
        let node_index = node_index
            .resolve(self)?
            .pop()
            .ok_or(GraphRecordError::NoNodeSelected)?;

        self.add_nodes(vec![(node_index, attributes)])
    }

    pub fn add_nodes_in_group(
        &self,
        source: impl NodeSource,
        group_index: impl SingleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let group_index = group_index
            .resolve(self)?
            .pop()
            .ok_or(GraphRecordError::NoGroupSelected)?;

        self.apply(AddNodesInGroup::new(source.collect_nodes()?, group_index).into())
    }

    pub fn add_node_in_group(
        &self,
        node_index: impl SingleSelection<NodeIndex>,
        attributes: impl IntoIterator<Item = (impl Into<AttributeName>, impl Into<Value>)>,
        group_index: impl SingleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let attributes = collect_attributes(attributes);
        let node_index = node_index
            .resolve(self)?
            .pop()
            .ok_or(GraphRecordError::NoNodeSelected)?;

        self.add_nodes_in_group(vec![(node_index, attributes)], group_index)
    }

    pub fn add_edges(&self, source: impl EdgeSource) -> GraphRecordResult<Self> {
        self.apply(AddEdges::new(source.collect_edges()?).into())
    }

    pub fn add_edge(
        &self,
        source_node_index: impl SingleSelection<NodeIndex>,
        target_node_index: impl SingleSelection<NodeIndex>,
        attributes: impl IntoIterator<Item = (impl Into<AttributeName>, impl Into<Value>)>,
    ) -> GraphRecordResult<Self> {
        let attributes = collect_attributes(attributes);
        let source_node_index = NodeIndex::verify(self, source_node_index.resolve(self)?)?
            .pop()
            .ok_or(GraphRecordError::NoNodeSelected)?;
        let target_node_index = NodeIndex::verify(self, target_node_index.resolve(self)?)?
            .pop()
            .ok_or(GraphRecordError::NoNodeSelected)?;

        self.add_edges(vec![(source_node_index, target_node_index, attributes)])
    }

    pub fn add_edges_in_group(
        &self,
        source: impl EdgeSource,
        group_index: impl SingleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let group_index = group_index
            .resolve(self)?
            .pop()
            .ok_or(GraphRecordError::NoGroupSelected)?;

        self.apply(AddEdgesInGroup::new(source.collect_edges()?, group_index).into())
    }

    pub fn add_edge_in_group(
        &self,
        source_node_index: impl SingleSelection<NodeIndex>,
        target_node_index: impl SingleSelection<NodeIndex>,
        attributes: impl IntoIterator<Item = (impl Into<AttributeName>, impl Into<Value>)>,
        group_index: impl SingleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let attributes = collect_attributes(attributes);
        let source_node_index = NodeIndex::verify(self, source_node_index.resolve(self)?)?
            .pop()
            .ok_or(GraphRecordError::NoNodeSelected)?;
        let target_node_index = NodeIndex::verify(self, target_node_index.resolve(self)?)?
            .pop()
            .ok_or(GraphRecordError::NoNodeSelected)?;

        self.add_edges_in_group(
            vec![(source_node_index, target_node_index, attributes)],
            group_index,
        )
    }

    pub fn remove_nodes(
        &self,
        node_indices: impl MultipleSelection<NodeIndex>,
    ) -> GraphRecordResult<Self> {
        self.apply(RemoveNodes::new(NodeIndex::verify(self, node_indices.resolve(self)?)?).into())
    }

    pub fn remove_edges(
        &self,
        edge_indices: impl MultipleSelection<EdgeIndex>,
    ) -> GraphRecordResult<Self> {
        self.apply(RemoveEdges::new(EdgeIndex::verify(self, edge_indices.resolve(self)?)?).into())
    }

    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn keep_nodes(
        &self,
        node_indices: impl MultipleSelection<NodeIndex>,
    ) -> GraphRecordResult<Self> {
        let keep_addresses: GrHashSet<_> = NodeIndex::verify(self, node_indices.resolve(self)?)?
            .into_iter()
            .map(|node_index| {
                self.state
                    .resolve_node_address(&node_index)
                    .expect("Node must exist.")
            })
            .collect();

        let nodes_to_remove: Vec<_> = self
            .state
            .node_addresses()
            .filter(|address| !keep_addresses.contains(address))
            .map(|address| {
                NodeIndex::from(Identifier::from(
                    self.state.node_key(address).expect("Node must exist."),
                ))
            })
            .collect();

        self.apply(RemoveNodes::new(nodes_to_remove).into())
    }

    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn keep_edges(
        &self,
        edge_indices: impl MultipleSelection<EdgeIndex>,
    ) -> GraphRecordResult<Self> {
        let keep_addresses: GrHashSet<_> = EdgeIndex::verify(self, edge_indices.resolve(self)?)?
            .into_iter()
            .map(|edge_index| {
                self.state
                    .resolve_edge_address(&edge_index)
                    .expect("Edge must exist.")
            })
            .collect();

        let edges_to_remove: Vec<_> = self
            .state
            .edge_addresses()
            .filter(|address| !keep_addresses.contains(address))
            .map(|address| self.state.edge_index(address).expect("Edge must exist."))
            .collect();

        self.apply(RemoveEdges::new(edges_to_remove).into())
    }

    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn keep_groups(
        &self,
        group_indices: impl MultipleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let keep_addresses: GrHashSet<_> = GroupIndex::verify(self, group_indices.resolve(self)?)?
            .into_iter()
            .map(|group_index| {
                self.state
                    .resolve_group_address(&group_index)
                    .expect("Group must exist.")
            })
            .collect();

        let groups_to_remove: Vec<_> = self
            .state
            .group_addresses()
            .filter(|address| !keep_addresses.contains(address))
            .map(|address| {
                self.state
                    .group_index(address)
                    .expect("Group must exist.")
                    .clone()
            })
            .collect();

        self.apply(RemoveGroups::new(groups_to_remove).into())
    }

    pub fn intersect(&self, other: &Self) -> GraphRecordResult<Self> {
        let nodes_to_remove: Vec<_> = self
            .node_indices()
            .map(NodeIndex::from)
            .filter(|node_index| !other.contains_node(node_index))
            .collect();

        self.apply(RemoveNodes::new(nodes_to_remove).into())
    }

    pub fn difference(&self, other: &Self) -> GraphRecordResult<Self> {
        let nodes_to_remove: Vec<_> = self
            .node_indices()
            .map(NodeIndex::from)
            .filter(|node_index| other.contains_node(node_index))
            .collect();

        self.apply(RemoveNodes::new(nodes_to_remove).into())
    }

    #[expect(clippy::missing_panics_doc, reason = "infallible")]
    pub fn merge(&self, other: &Self, on_conflict: OnConflict) -> GraphRecordResult<Self> {
        let mut changes = Changes::new();

        let mut nodes_to_add = Vec::new();
        let mut node_attribute_updates = Vec::new();

        for node_index_view in other.node_indices() {
            let node_index = NodeIndex::from(node_index_view);
            let other_node = other.node(&node_index).expect("Node must exist.");

            if !self.contains_node(&node_index) {
                let attributes = other_node
                    .attributes()
                    .map(|(attribute_name, value)| (attribute_name.clone(), Value::from(value)))
                    .collect();

                nodes_to_add.push((node_index, attributes));
                continue;
            }

            let self_node = self.node(&node_index).expect("Node must exist.");
            let mut attribute_updates = AttributeMap::new();

            for (attribute_name, other_value) in other_node.attributes() {
                let Some(self_value) = self_node.attribute(attribute_name) else {
                    attribute_updates.insert(attribute_name.clone(), Value::from(other_value));
                    continue;
                };

                if self_value == other_value {
                    continue;
                }

                match on_conflict {
                    OnConflict::Raise => {
                        return Err(GraphRecordError::NodeAttributeConflict {
                            node_index,
                            attribute_name: attribute_name.clone(),
                            self_value: Value::from(self_value),
                            other_value: Value::from(other_value),
                        });
                    }
                    OnConflict::KeepSelf => {}
                    OnConflict::KeepOther => {
                        attribute_updates.insert(attribute_name.clone(), Value::from(other_value));
                    }
                }
            }

            if !attribute_updates.is_empty() {
                node_attribute_updates.push((node_index, attribute_updates));
            }
        }

        if !nodes_to_add.is_empty() {
            changes.push(AddNodes::new(NodeBatch::from(nodes_to_add)));
        }

        for (node_index, attribute_updates) in node_attribute_updates {
            changes.push(SetNodeAttributes::new(vec![node_index], attribute_updates));
        }

        let mut ungrouped_edges_to_add = Vec::new();
        let mut grouped_edges_to_add: GrHashMap<
            GroupIndex,
            Vec<(NodeIndex, NodeIndex, AttributeMap)>,
        > = GrHashMap::default();
        let mut remaining_edge_memberships = Vec::new();

        for edge_index in other.edge_indices() {
            if self.contains_edge(&edge_index) {
                continue;
            }

            let other_edge = other.edge(&edge_index).expect("Edge must exist.");
            let attributes: AttributeMap = other_edge
                .attributes()
                .map(|(attribute_name, value)| (attribute_name.clone(), Value::from(value)))
                .collect();
            let edge = (
                NodeIndex::from(other_edge.source()),
                NodeIndex::from(other_edge.target()),
                attributes,
            );
            let mut group_indices = other_edge.groups().cloned();

            match group_indices.next() {
                None => ungrouped_edges_to_add.push(edge),
                Some(first_group_index) => {
                    let remaining: Vec<GroupIndex> = group_indices.collect();

                    if !remaining.is_empty() {
                        remaining_edge_memberships.push((
                            first_group_index.clone(),
                            edge.clone(),
                            remaining,
                        ));
                    }

                    grouped_edges_to_add
                        .entry(first_group_index)
                        .or_default()
                        .push(edge);
                }
            }
        }

        if !ungrouped_edges_to_add.is_empty() {
            changes.push(AddEdges::new(EdgeBatch::from(ungrouped_edges_to_add)));
        }

        for group_address in StateView::of(other).group_addresses() {
            let group_index = StateView::of(other).group_index(group_address).clone();
            let other_group = other.group(&group_index).expect("Group must exist.");
            let self_group = self.group(&group_index).ok();

            if self_group.is_none() {
                changes.push(AddGroup::new(group_index.clone()));
            }

            let self_node_members: GrHashSet<_> = self_group
                .map(|group| group.nodes().map(NodeIndex::from).collect())
                .unwrap_or_default();
            let nodes_to_add_to_group: Vec<_> = other_group
                .nodes()
                .map(NodeIndex::from)
                .filter(|node_index| !self_node_members.contains(node_index))
                .collect();

            if !nodes_to_add_to_group.is_empty() {
                changes.push(AddNodesToGroup::new(
                    nodes_to_add_to_group,
                    group_index.clone(),
                ));
            }

            let self_edge_members: GrHashSet<_> = self_group
                .map(|group| group.edges().collect())
                .unwrap_or_default();
            let edges_to_add_to_group: Vec<_> = other_group
                .edges()
                .filter(|edge_index| {
                    self.contains_edge(edge_index) && !self_edge_members.contains(edge_index)
                })
                .collect();

            if !edges_to_add_to_group.is_empty() {
                changes.push(AddEdgesToGroup::new(edges_to_add_to_group, group_index));
            }
        }

        for (group_index, edges) in grouped_edges_to_add {
            changes.push(AddEdgesInGroup::new(EdgeBatch::from(edges), group_index));
        }

        let merged = if changes.is_empty() {
            self.clone()
        } else {
            self.apply(changes)?
        };

        if remaining_edge_memberships.is_empty() {
            return Ok(merged);
        }

        let mut minted_by_group: GrHashMap<GroupIndex, Vec<EdgeIndex>> = GrHashMap::default();

        for (first_group_index, _, _) in &remaining_edge_memberships {
            if minted_by_group.contains_key(first_group_index) {
                continue;
            }

            let mut minted: Vec<EdgeIndex> = merged
                .group(first_group_index)?
                .edges()
                .filter(|edge_index| !self.contains_edge(edge_index))
                .collect();
            minted.sort_by_key(|edge_index| (edge_index.tag(), edge_index.offset()));
            minted_by_group.insert(first_group_index.clone(), minted);
        }

        let mut memberships_by_group: GrHashMap<GroupIndex, Vec<EdgeIndex>> = GrHashMap::default();

        for (first_group_index, edge, remaining) in remaining_edge_memberships {
            let candidates = minted_by_group
                .get_mut(&first_group_index)
                .expect("Candidates must exist.");
            let matched = candidates.iter().position(|edge_index| {
                let candidate = merged.edge(edge_index).expect("Edge must exist.");
                let (source, target, attributes) = &edge;

                NodeIndex::from(candidate.source()) == *source
                    && NodeIndex::from(candidate.target()) == *target
                    && candidate
                        .attributes()
                        .map(|(attribute_name, value)| (attribute_name.clone(), Value::from(value)))
                        .collect::<AttributeMap>()
                        == *attributes
            });

            let Some(position) = matched else {
                continue;
            };
            let edge_index = candidates.swap_remove(position);

            for group_index in remaining {
                memberships_by_group
                    .entry(group_index)
                    .or_default()
                    .push(edge_index);
            }
        }

        let mut membership_changes = Changes::new();

        for (group_index, edge_indices) in memberships_by_group {
            membership_changes.push(AddEdgesToGroup::new(edge_indices, group_index));
        }

        if membership_changes.is_empty() {
            return Ok(merged);
        }

        merged.apply(membership_changes)
    }

    pub fn set_node_attributes(
        &self,
        node_indices: impl MultipleSelection<NodeIndex>,
        attributes: impl IntoIterator<Item = (impl Into<AttributeName>, impl Into<Value>)>,
    ) -> GraphRecordResult<Self> {
        let attributes = collect_attributes(attributes);

        self.apply(
            SetNodeAttributes::new(
                NodeIndex::verify(self, node_indices.resolve(self)?)?,
                attributes,
            )
            .into(),
        )
    }

    pub fn replace_node_attributes(
        &self,
        node_indices: impl MultipleSelection<NodeIndex>,
        attributes: impl IntoIterator<Item = (impl Into<AttributeName>, impl Into<Value>)>,
    ) -> GraphRecordResult<Self> {
        let attributes = collect_attributes(attributes);

        self.apply(
            ReplaceNodeAttributes::new(
                NodeIndex::verify(self, node_indices.resolve(self)?)?,
                attributes,
            )
            .into(),
        )
    }

    pub fn remove_node_attributes(
        &self,
        node_indices: impl MultipleSelection<NodeIndex>,
        attribute_names: impl IntoIterator<Item = impl Into<AttributeName>>,
    ) -> GraphRecordResult<Self> {
        let attribute_names: Vec<_> = attribute_names.into_iter().map(Into::into).collect();

        self.apply(
            RemoveNodeAttributes::new(
                NodeIndex::verify(self, node_indices.resolve(self)?)?,
                attribute_names,
            )
            .into(),
        )
    }

    pub fn set_edge_attributes(
        &self,
        edge_indices: impl MultipleSelection<EdgeIndex>,
        attributes: impl IntoIterator<Item = (impl Into<AttributeName>, impl Into<Value>)>,
    ) -> GraphRecordResult<Self> {
        let attributes = collect_attributes(attributes);

        self.apply(
            SetEdgeAttributes::new(
                EdgeIndex::verify(self, edge_indices.resolve(self)?)?,
                attributes,
            )
            .into(),
        )
    }

    pub fn replace_edge_attributes(
        &self,
        edge_indices: impl MultipleSelection<EdgeIndex>,
        attributes: impl IntoIterator<Item = (impl Into<AttributeName>, impl Into<Value>)>,
    ) -> GraphRecordResult<Self> {
        let attributes = collect_attributes(attributes);

        self.apply(
            ReplaceEdgeAttributes::new(
                EdgeIndex::verify(self, edge_indices.resolve(self)?)?,
                attributes,
            )
            .into(),
        )
    }

    pub fn remove_edge_attributes(
        &self,
        edge_indices: impl MultipleSelection<EdgeIndex>,
        attribute_names: impl IntoIterator<Item = impl Into<AttributeName>>,
    ) -> GraphRecordResult<Self> {
        let attribute_names: Vec<_> = attribute_names.into_iter().map(Into::into).collect();

        self.apply(
            RemoveEdgeAttributes::new(
                EdgeIndex::verify(self, edge_indices.resolve(self)?)?,
                attribute_names,
            )
            .into(),
        )
    }

    pub fn add_group(
        &self,
        group_index: impl SingleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let group_index = group_index
            .resolve(self)?
            .pop()
            .ok_or(GraphRecordError::NoGroupSelected)?;

        self.apply(AddGroup::new(group_index).into())
    }

    pub fn remove_groups(
        &self,
        group_indices: impl MultipleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        self.apply(
            RemoveGroups::new(GroupIndex::verify(self, group_indices.resolve(self)?)?).into(),
        )
    }

    pub fn add_nodes_to_group(
        &self,
        node_indices: impl MultipleSelection<NodeIndex>,
        group_index: impl SingleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let node_indices = NodeIndex::verify(self, node_indices.resolve(self)?)?;
        let group_index = group_index
            .resolve(self)?
            .pop()
            .ok_or(GraphRecordError::NoGroupSelected)?;

        self.apply(AddNodesToGroup::new(node_indices, group_index).into())
    }

    pub fn remove_nodes_from_group(
        &self,
        node_indices: impl MultipleSelection<NodeIndex>,
        group_index: impl SingleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let node_indices = NodeIndex::verify(self, node_indices.resolve(self)?)?;
        let group_index = GroupIndex::verify(self, group_index.resolve(self)?)?
            .pop()
            .ok_or(GraphRecordError::NoGroupSelected)?;

        self.apply(RemoveNodesFromGroup::new(node_indices, group_index).into())
    }

    pub fn add_edges_to_group(
        &self,
        edge_indices: impl MultipleSelection<EdgeIndex>,
        group_index: impl SingleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let edge_indices = EdgeIndex::verify(self, edge_indices.resolve(self)?)?;
        let group_index = group_index
            .resolve(self)?
            .pop()
            .ok_or(GraphRecordError::NoGroupSelected)?;

        self.apply(AddEdgesToGroup::new(edge_indices, group_index).into())
    }

    pub fn remove_edges_from_group(
        &self,
        edge_indices: impl MultipleSelection<EdgeIndex>,
        group_index: impl SingleSelection<GroupIndex>,
    ) -> GraphRecordResult<Self> {
        let edge_indices = EdgeIndex::verify(self, edge_indices.resolve(self)?)?;
        let group_index = GroupIndex::verify(self, group_index.resolve(self)?)?
            .pop()
            .ok_or(GraphRecordError::NoGroupSelected)?;

        self.apply(RemoveEdgesFromGroup::new(edge_indices, group_index).into())
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
    pub fn contains_node<'a>(&self, node_index: impl Into<NodeIndexView<'a>>) -> bool {
        self.state.resolve_node_address(node_index).is_some()
    }

    #[must_use]
    pub fn contains_edge(&self, edge_index: &EdgeIndex) -> bool {
        self.state.resolve_edge_address(edge_index).is_some()
    }

    #[must_use]
    pub fn contains_group<'a>(&self, group_index: impl Into<GroupIndexView<'a>>) -> bool {
        self.state.resolve_group_address(group_index).is_some()
    }

    #[must_use]
    pub fn schema(&self) -> &Schema {
        self.state.schema()
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

    pub fn group_indices(&self) -> impl Iterator<Item = &GroupIndex> {
        let state = StateView::of(self);

        state
            .group_addresses()
            .map(move |group_address| state.group_index(group_address))
    }

    pub fn node<'a>(
        &self,
        node_index: impl Into<NodeIndexView<'a>>,
    ) -> GraphRecordResult<NodeView<'_>> {
        NodeView::new(self, node_index.into())
    }

    pub fn edge(&self, edge_index: &EdgeIndex) -> GraphRecordResult<EdgeView<'_>> {
        EdgeView::new(self, edge_index)
    }

    pub fn group<'a>(
        &self,
        group_index: impl Into<GroupIndexView<'a>>,
    ) -> GraphRecordResult<GroupView<'_>> {
        GroupView::new(self, group_index.into())
    }
}

impl PartialEq for GraphRecord {
    fn eq(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.state, &other.state) {
            return true;
        }

        if self.schema() != other.schema()
            || self.node_count() != other.node_count()
            || self.edge_count() != other.edge_count()
            || self.group_count() != other.group_count()
        {
            return false;
        }

        let group_indices: GrHashSet<_> = self.group_indices().collect();
        if !other
            .group_indices()
            .all(|group_index| group_indices.contains(group_index))
        {
            return false;
        }

        for node_index in self.node_indices() {
            let lookups = (self.node(node_index.clone()), other.node(node_index));
            let (Ok(node), Ok(other_node)) = lookups else {
                return false;
            };

            let mut attribute_count = 0;
            for (attribute_name, value) in node.attributes() {
                attribute_count += 1;

                if other_node.attribute(attribute_name) != Some(value) {
                    return false;
                }
            }
            if attribute_count != other_node.attributes().count() {
                return false;
            }

            let groups: GrHashSet<_> = node.groups().collect();
            if groups.len() != other_node.groups().count()
                || !other_node
                    .groups()
                    .all(|group_index| groups.contains(group_index))
            {
                return false;
            }
        }

        for edge_index in self.edge_indices() {
            let lookups = (self.edge(&edge_index), other.edge(&edge_index));
            let (Ok(edge), Ok(other_edge)) = lookups else {
                return false;
            };

            if edge.source() != other_edge.source() || edge.target() != other_edge.target() {
                return false;
            }

            let mut attribute_count = 0;
            for (attribute_name, value) in edge.attributes() {
                attribute_count += 1;

                if other_edge.attribute(attribute_name) != Some(value) {
                    return false;
                }
            }
            if attribute_count != other_edge.attributes().count() {
                return false;
            }

            let groups: GrHashSet<_> = edge.groups().collect();
            if groups.len() != other_edge.groups().count()
                || !other_edge
                    .groups()
                    .all(|group_index| groups.contains(group_index))
            {
                return false;
            }
        }

        true
    }
}

impl fmt::Debug for GraphRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GraphRecord")
            .field("node_count", &self.node_count())
            .field("edge_count", &self.edge_count())
            .field("group_count", &self.group_count())
            .finish()
    }
}

impl Default for GraphRecord {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::{AttributeMap, GraphRecord, Identifier, NodeIndexView, OnConflict};
    use crate::{
        errors::GraphRecordError,
        graphrecord::{
            EdgeIndex, GroupIndex, NodeIndex, Value,
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
            .add_node("lorem", create_lorem_attributes())
            .unwrap()
            .add_node("ipsum", AttributeMap::new())
            .unwrap()
    }

    fn create_graphrecord_with_one_edge() -> (GraphRecord, EdgeIndex) {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_edge("lorem", "ipsum", create_lorem_attributes())
            .unwrap();
        let edge_index = graphrecord.edge_indices().next().unwrap();

        (graphrecord, edge_index)
    }

    fn create_overlapping_graphrecords() -> (GraphRecord, GraphRecord) {
        let graphrecord = GraphRecord::new()
            .add_node("lorem", AttributeMap::new())
            .unwrap()
            .add_node("ipsum", create_lorem_attributes())
            .unwrap()
            .add_node("dolor", AttributeMap::new())
            .unwrap()
            .add_edge("lorem", "ipsum", AttributeMap::new())
            .unwrap()
            .add_edge("ipsum", "dolor", AttributeMap::new())
            .unwrap();
        let other = GraphRecord::new()
            .add_node("ipsum", AttributeMap::from([("lorem".into(), 7.into())]))
            .unwrap()
            .add_node("dolor", AttributeMap::new())
            .unwrap()
            .add_node("sit", AttributeMap::new())
            .unwrap();

        (graphrecord, other)
    }

    fn create_mergeable_graphrecords() -> (GraphRecord, GraphRecord, EdgeIndex) {
        let base = GraphRecord::new()
            .add_group("amet")
            .unwrap()
            .add_node_in_group("lorem", AttributeMap::new(), "amet")
            .unwrap()
            .add_nodes(vec![
                ("ipsum", create_lorem_attributes()),
                ("dolor", AttributeMap::new()),
            ])
            .unwrap()
            .add_edge("lorem", "ipsum", AttributeMap::new())
            .unwrap();

        let shared_edge_index = base.edge_indices().next().unwrap();
        let graphrecord = base.clone();

        let other = base
            .replace_node_attributes(
                vec!["ipsum"],
                AttributeMap::from([("lorem".into(), 7.into()), ("sed".into(), true.into())]),
            )
            .unwrap()
            .add_nodes_to_group(vec!["ipsum"], "amet")
            .unwrap()
            .add_group("consectetur")
            .unwrap()
            .add_node("sit", AttributeMap::new())
            .unwrap()
            .add_edges(vec![("ipsum", "dolor", AttributeMap::new())])
            .unwrap()
            .add_edges_to_group(vec![shared_edge_index], "consectetur")
            .unwrap();

        (graphrecord, other, shared_edge_index)
    }

    #[test]
    fn test_new() {
        let graphrecord = GraphRecord::new();

        assert_eq!(0, graphrecord.node_count());
        assert_eq!(0, graphrecord.edge_count());
        assert_eq!(0, graphrecord.group_count());
    }

    #[test]
    fn test_with_schema() {
        let schema = Schema::new_provided(HashMap::new(), GroupSchema::default());

        let graphrecord = GraphRecord::with_schema(schema.clone());

        assert_eq!(0, graphrecord.node_count());
        assert_eq!(0, graphrecord.edge_count());
        assert_eq!(0, graphrecord.group_count());
        assert_eq!(&schema, graphrecord.schema());
    }

    #[test]
    fn test_add_nodes() {
        let graphrecord = create_graphrecord_with_two_nodes();

        assert_eq!(2, graphrecord.node_count());
        assert!(graphrecord.contains_node("lorem"));
        assert!(graphrecord.contains_node("ipsum"));
        assert_eq!(
            Some(42.into()),
            graphrecord
                .node("lorem")
                .unwrap()
                .attribute("lorem")
                .map(Value::from)
        );

        let derived = graphrecord.add_node("dolor", AttributeMap::new()).unwrap();

        assert_eq!(2, graphrecord.node_count());
        assert!(!graphrecord.contains_node("dolor"));
        assert_eq!(3, derived.node_count());
        assert!(!Arc::ptr_eq(graphrecord.state(), derived.state()));

        let derived = derived.add_node("sit", [("amet", 7)]).unwrap();

        assert_eq!(
            Some(7.into()),
            derived
                .node("sit")
                .unwrap()
                .attribute("amet")
                .map(Value::from)
        );
    }

    #[test]
    fn test_invalid_add_nodes() {
        let original = create_graphrecord_with_two_nodes();

        let result = original.add_nodes(vec![
            ("dolor", AttributeMap::new()),
            ("lorem", AttributeMap::new()),
        ]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeAlreadyExists { node_index }
                if node_index == "lorem".into()
        )));
        assert_eq!(2, original.node_count());
        assert!(!original.contains_node("dolor"));
    }

    #[test]
    fn test_add_nodes_in_group() {
        let graphrecord = GraphRecord::new()
            .add_group("dolor")
            .unwrap()
            .add_nodes_in_group(vec![("lorem", AttributeMap::new())], "dolor")
            .unwrap();

        assert!(graphrecord.contains_node("lorem"));

        let created = GraphRecord::new()
            .add_nodes_in_group(vec![("lorem", AttributeMap::new())], "sit")
            .unwrap();

        assert!(created.contains_group("sit"));
        assert_eq!(1, created.group("sit").unwrap().node_count());

        let state = graphrecord.state();
        let node_address = state.resolve_node_address("lorem").unwrap();
        let group_address = state.resolve_group_address("dolor").unwrap();

        assert!(
            state
                .node_memberships(node_address)
                .any(|membership| membership == group_address)
        );
        assert!(
            graphrecord
                .add_nodes_to_group(vec!["lorem"], "dolor")
                .is_err_and(|error| matches!(error, GraphRecordError::NodeAlreadyInGroup { .. }))
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
                .edge(&edge_index)
                .unwrap()
                .attribute("lorem")
                .map(Value::from)
        );

        let graphrecord = create_graphrecord_with_two_nodes()
            .add_edges(vec![
                ("lorem", "ipsum", create_lorem_attributes()),
                ("ipsum", "lorem", AttributeMap::new()),
                ("lorem", "lorem", AttributeMap::new()),
            ])
            .unwrap();

        let derived = graphrecord
            .add_edge("lorem", "ipsum", [("sed", 7)])
            .unwrap();

        assert_eq!(4, derived.edge_count());

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
        let result =
            create_graphrecord_with_two_nodes().add_edge("lorem", "dolor", AttributeMap::new());

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodesNotFound { node_indices }
                if node_indices == vec![NodeIndex::from("dolor")]
        )));

        let original = create_graphrecord_with_two_nodes();

        let result = original.add_edges(vec![
            ("lorem", "ipsum", AttributeMap::new()),
            ("lorem", "dolor", AttributeMap::new()),
        ]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeNotFound { node_index }
                if node_index == "dolor".into()
        )));
        assert_eq!(0, original.edge_count());
    }

    #[test]
    fn test_add_edges_in_group() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor")
            .unwrap()
            .add_edges_in_group(vec![("lorem", "ipsum", AttributeMap::new())], "dolor")
            .unwrap();
        let edge_index = graphrecord.edge_indices().next().unwrap();

        let created = create_graphrecord_with_two_nodes()
            .add_edges_in_group(vec![("lorem", "ipsum", AttributeMap::new())], "sit")
            .unwrap();

        assert!(created.contains_group("sit"));
        assert_eq!(1, created.group("sit").unwrap().edge_count());

        let state = graphrecord.state();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let group_address = state.resolve_group_address("dolor").unwrap();

        assert!(
            state
                .edge_memberships(edge_address)
                .any(|membership| membership == group_address)
        );
        assert!(
            graphrecord
                .add_edges_to_group(vec![edge_index], "dolor")
                .is_err_and(|error| matches!(error, GraphRecordError::EdgeAlreadyInGroup { .. }))
        );
    }

    #[test]
    fn test_remove_nodes() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let removed = graphrecord.remove_nodes(vec!["lorem"]).unwrap();

        assert_eq!(1, removed.node_count());
        assert!(!removed.contains_node("lorem"));
        assert_eq!(0, removed.edge_count());
        assert!(!removed.contains_edge(&edge_index));
        assert_eq!(2, graphrecord.node_count());

        let graphrecord = create_graphrecord_with_two_nodes()
            .remove_nodes(vec!["lorem"])
            .unwrap()
            .add_node("lorem", AttributeMap::from([("sed".into(), 7.into())]))
            .unwrap();

        assert_eq!(2, graphrecord.node_count());
        assert_eq!(
            Some(7.into()),
            graphrecord
                .node("lorem")
                .unwrap()
                .attribute("sed")
                .map(Value::from)
        );
    }

    #[test]
    fn test_invalid_remove_nodes() {
        let result = create_graphrecord_with_two_nodes().remove_nodes(vec!["dolor", "sit"]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodesNotFound { node_indices }
                if node_indices == vec![NodeIndex::from("dolor"), NodeIndex::from("sit")]
        )));
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

        let result = removed.remove_edges(vec![edge_index]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::EdgesNotFound { edge_indices }
                if edge_indices == vec![edge_index]
        )));
    }

    #[test]
    fn test_keep_nodes() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_node("dolor", AttributeMap::new())
            .unwrap();

        let kept = graphrecord.keep_nodes(["lorem"]).unwrap();

        assert_eq!(1, kept.node_count());
        assert!(kept.contains_node("lorem"));
        assert!(!kept.contains_node("ipsum"));

        let kept = create_graphrecord_with_two_nodes()
            .keep_nodes(Vec::<NodeIndex>::new())
            .unwrap();

        assert_eq!(0, kept.node_count());
    }

    #[test]
    fn test_invalid_keep_nodes() {
        let result = create_graphrecord_with_two_nodes().keep_nodes(["dolor"]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodesNotFound { node_indices }
                if node_indices == vec![NodeIndex::from("dolor")]
        )));
    }

    #[test]
    fn test_keep_edges() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_node("dolor", AttributeMap::new())
            .unwrap()
            .add_edge("lorem", "ipsum", AttributeMap::new())
            .unwrap()
            .add_edge("ipsum", "dolor", AttributeMap::new())
            .unwrap();
        let first_edge_index = graphrecord.edge_indices().next().unwrap();

        let kept = graphrecord.keep_edges([first_edge_index]).unwrap();

        assert_eq!(1, kept.edge_count());
        assert!(kept.contains_edge(&first_edge_index));

        let (graphrecord, _) = create_graphrecord_with_one_edge();

        let kept = graphrecord.keep_edges(Vec::<EdgeIndex>::new()).unwrap();

        assert_eq!(0, kept.edge_count());
        assert_eq!(2, kept.node_count());
    }

    #[test]
    fn test_invalid_keep_edges() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let removed = graphrecord.remove_edges(vec![edge_index]).unwrap();

        let result = removed.keep_edges([edge_index]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::EdgesNotFound { edge_indices }
                if edge_indices == vec![edge_index]
        )));
    }

    #[test]
    fn test_keep_groups() {
        let graphrecord = GraphRecord::new()
            .add_group("lorem")
            .unwrap()
            .add_group("ipsum")
            .unwrap();

        let kept = graphrecord.keep_groups(["lorem"]).unwrap();

        assert_eq!(1, kept.group_count());
        assert!(kept.contains_group("lorem"));
        assert!(!kept.contains_group("ipsum"));

        let graphrecord = GraphRecord::new().add_group("lorem").unwrap();

        let kept = graphrecord.keep_groups(Vec::<GroupIndex>::new()).unwrap();

        assert_eq!(0, kept.group_count());
    }

    #[test]
    fn test_invalid_keep_groups() {
        let result = GraphRecord::new().keep_groups(["lorem"]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::GroupsNotFound { group_indices }
                if group_indices == vec![GroupIndex::from("lorem")]
        )));
    }

    #[test]
    fn test_intersect() {
        let (graphrecord, other) = create_overlapping_graphrecords();

        let common = graphrecord.intersect(&other).unwrap();

        assert_eq!(2, common.node_count());
        assert!(common.contains_node("ipsum"));
        assert!(common.contains_node("dolor"));
        assert!(!common.contains_node("lorem"));
        assert!(!common.contains_node("sit"));
        assert_eq!(3, graphrecord.node_count());
        assert_eq!(
            Some(42.into()),
            common
                .node("ipsum")
                .unwrap()
                .attribute("lorem")
                .map(Value::from)
        );
        assert_eq!(1, common.edge_count());

        let induced_edge_index = common.edge_indices().next().unwrap();

        assert_eq!(
            (
                NodeIndexView::from(&Identifier::from("ipsum")),
                NodeIndexView::from(&Identifier::from("dolor"))
            ),
            {
                let edge = common.edge(&induced_edge_index).unwrap();
                (edge.source(), edge.target())
            }
        );

        let disjoint = GraphRecord::new()
            .add_node("sit", AttributeMap::new())
            .unwrap();

        let common = graphrecord.intersect(&disjoint).unwrap();

        assert_eq!(0, common.node_count());
        assert_eq!(0, common.edge_count());

        let common = graphrecord.intersect(&graphrecord).unwrap();

        assert_eq!(3, common.node_count());
        assert_eq!(2, common.edge_count());
    }

    #[test]
    fn test_difference() {
        let (graphrecord, other) = create_overlapping_graphrecords();

        let subtracted = graphrecord.difference(&other).unwrap();

        assert_eq!(1, subtracted.node_count());
        assert!(subtracted.contains_node("lorem"));
        assert!(!subtracted.contains_node("ipsum"));
        assert!(!subtracted.contains_node("dolor"));
        assert!(!subtracted.contains_node("sit"));
        assert_eq!(0, subtracted.edge_count());
        assert_eq!(3, graphrecord.node_count());

        let disjoint = GraphRecord::new()
            .add_node("sit", AttributeMap::new())
            .unwrap();

        let subtracted = graphrecord.difference(&disjoint).unwrap();

        assert_eq!(3, subtracted.node_count());
        assert_eq!(2, subtracted.edge_count());
        assert_eq!(
            Some(42.into()),
            subtracted
                .node("ipsum")
                .unwrap()
                .attribute("lorem")
                .map(Value::from)
        );

        let subtracted = graphrecord.difference(&graphrecord).unwrap();

        assert_eq!(0, subtracted.node_count());
        assert_eq!(0, subtracted.edge_count());
    }

    #[test]
    fn test_merge() {
        let (graphrecord, other, shared_edge_index) = create_mergeable_graphrecords();

        let merged = graphrecord.merge(&other, OnConflict::KeepOther).unwrap();

        assert_eq!(4, merged.node_count());
        assert!(merged.contains_node("sit"));
        assert_eq!(
            Some(7.into()),
            merged
                .node("ipsum")
                .unwrap()
                .attribute("lorem")
                .map(Value::from)
        );
        assert_eq!(
            Some(true.into()),
            merged
                .node("ipsum")
                .unwrap()
                .attribute("sed")
                .map(Value::from)
        );
        assert_eq!(2, merged.edge_count());
        assert!(merged.contains_edge(&shared_edge_index));

        let new_edge_index = merged
            .edge_indices()
            .find(|edge_index| *edge_index != shared_edge_index)
            .unwrap();

        assert_eq!(
            (
                NodeIndexView::from(&Identifier::from("ipsum")),
                NodeIndexView::from(&Identifier::from("dolor"))
            ),
            {
                let edge = merged.edge(&new_edge_index).unwrap();
                (edge.source(), edge.target())
            }
        );
        assert!(merged.contains_group("consectetur"));
        assert!(
            merged
                .group("consectetur")
                .unwrap()
                .edges()
                .any(|edge_index| edge_index == shared_edge_index)
        );
        assert!(
            merged
                .group("amet")
                .unwrap()
                .nodes()
                .any(|node_index| node_index == NodeIndexView::from(&Identifier::from("ipsum")))
        );
        assert!(
            merged
                .group("amet")
                .unwrap()
                .nodes()
                .any(|node_index| node_index == NodeIndexView::from(&Identifier::from("lorem")))
        );

        let grouped_other = other
            .add_edge_in_group("sit", "dolor", AttributeMap::new(), "consectetur")
            .unwrap();
        let multi_grouped_edge_index = grouped_other
            .group("consectetur")
            .unwrap()
            .edges()
            .find(|edge_index| *edge_index != shared_edge_index)
            .unwrap();
        let grouped_other = grouped_other
            .add_group("elit")
            .unwrap()
            .add_edges_to_group(vec![multi_grouped_edge_index], "elit")
            .unwrap();
        let regrouped = graphrecord
            .merge(&grouped_other, OnConflict::KeepOther)
            .unwrap();
        let minted_edge_index = regrouped
            .group("consectetur")
            .unwrap()
            .edges()
            .find(|edge_index| *edge_index != shared_edge_index)
            .unwrap();

        assert_eq!(
            (
                NodeIndexView::from(&Identifier::from("sit")),
                NodeIndexView::from(&Identifier::from("dolor"))
            ),
            {
                let minted_edge = regrouped.edge(&minted_edge_index).unwrap();
                (minted_edge.source(), minted_edge.target())
            }
        );
        assert!(
            regrouped
                .group("elit")
                .unwrap()
                .edges()
                .any(|edge_index| edge_index == minted_edge_index)
        );

        let kept_self = graphrecord.merge(&other, OnConflict::KeepSelf).unwrap();

        assert_eq!(
            Some(42.into()),
            kept_self
                .node("ipsum")
                .unwrap()
                .attribute("lorem")
                .map(Value::from)
        );
        assert_eq!(
            Some(true.into()),
            kept_self
                .node("ipsum")
                .unwrap()
                .attribute("sed")
                .map(Value::from)
        );

        let no_op = graphrecord.merge(&graphrecord, OnConflict::Raise).unwrap();

        assert!(Arc::ptr_eq(graphrecord.state(), no_op.state()));
    }

    #[test]
    fn test_invalid_merge() {
        let (graphrecord, other, _) = create_mergeable_graphrecords();

        let result = graphrecord.merge(&other, OnConflict::Raise);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeAttributeConflict {
                node_index,
                attribute_name,
                self_value,
                other_value,
            } if node_index == "ipsum".into()
                && attribute_name == "lorem".into()
                && self_value == 42.into()
                && other_value == 7.into()
        )));
        assert_eq!(3, graphrecord.node_count());
    }

    #[test]
    fn test_set_node_attributes() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .set_node_attributes(vec!["ipsum"], [("sed", true)])
            .unwrap();

        assert_eq!(
            Some(true.into()),
            graphrecord
                .node("ipsum")
                .unwrap()
                .attribute("sed")
                .map(Value::from)
        );
    }

    #[test]
    fn test_replace_node_attributes() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .replace_node_attributes(vec!["lorem"], AttributeMap::new())
            .unwrap();

        assert_eq!(None, graphrecord.node("lorem").unwrap().attribute("lorem"));
    }

    #[test]
    fn test_remove_node_attributes() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .remove_node_attributes(vec!["lorem"], vec!["lorem"])
            .unwrap();

        assert_eq!(None, graphrecord.node("lorem").unwrap().attribute("lorem"));
    }

    #[test]
    fn test_invalid_remove_node_attributes() {
        let result =
            create_graphrecord_with_two_nodes().remove_node_attributes(vec!["ipsum"], vec!["sed"]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeAttributeNotFound {
                node_index,
                attribute_name
            } if node_index == "ipsum".into() && attribute_name == "sed".into()
        )));
    }

    #[test]
    fn test_set_edge_attributes() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .set_edge_attributes(vec![edge_index], [("sed", true)])
            .unwrap();

        assert_eq!(
            Some(true.into()),
            graphrecord
                .edge(&edge_index)
                .unwrap()
                .attribute("sed")
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
            graphrecord.edge(&edge_index).unwrap().attribute("lorem")
        );
    }

    #[test]
    fn test_remove_edge_attributes() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .remove_edge_attributes(vec![edge_index], vec!["lorem"])
            .unwrap();

        assert_eq!(
            None,
            graphrecord.edge(&edge_index).unwrap().attribute("lorem")
        );
    }

    #[test]
    fn test_add_group() {
        let graphrecord = GraphRecord::new().add_group("lorem").unwrap();

        assert_eq!(1, graphrecord.group_count());
        assert!(graphrecord.contains_group("lorem"));
    }

    #[test]
    fn test_invalid_add_group() {
        let result = GraphRecord::new()
            .add_group("lorem")
            .unwrap()
            .add_group("lorem");

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::GroupAlreadyExists { group_index }
                if group_index == "lorem".into()
        )));
    }

    #[test]
    fn test_remove_groups() {
        let graphrecord = GraphRecord::new()
            .add_group("lorem")
            .unwrap()
            .remove_groups(vec!["lorem"])
            .unwrap();

        assert_eq!(0, graphrecord.group_count());
    }

    #[test]
    fn test_invalid_remove_groups() {
        let result = GraphRecord::new().remove_groups(vec!["lorem"]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::GroupsNotFound { group_indices }
                if group_indices == vec![GroupIndex::from("lorem")]
        )));
    }

    #[test]
    fn test_add_nodes_to_group() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor")
            .unwrap()
            .add_nodes_to_group(vec!["lorem"], "dolor")
            .unwrap();

        let state = graphrecord.state();
        let node_address = state.resolve_node_address("lorem").unwrap();
        let group_address = state.resolve_group_address("dolor").unwrap();

        assert!(
            state
                .node_memberships(node_address)
                .any(|membership| membership == group_address)
        );
    }

    #[test]
    fn test_invalid_add_nodes_to_group() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor")
            .unwrap()
            .add_nodes_to_group(vec!["lorem"], "dolor")
            .unwrap();

        assert!(
            graphrecord
                .add_nodes_to_group(vec!["lorem"], "dolor")
                .is_err_and(|error| matches!(error, GraphRecordError::NodeAlreadyInGroup { .. }))
        );
    }

    #[test]
    fn test_remove_nodes_from_group() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor")
            .unwrap()
            .add_nodes_to_group(vec!["lorem"], "dolor")
            .unwrap()
            .remove_nodes_from_group(vec!["lorem"], "dolor")
            .unwrap();

        let state = graphrecord.state();
        let node_address = state.resolve_node_address("lorem").unwrap();
        let group_address = state.resolve_group_address("dolor").unwrap();

        assert!(
            !state
                .node_memberships(node_address)
                .any(|membership| membership == group_address)
        );
    }

    #[test]
    fn test_invalid_remove_nodes_from_group() {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_group("dolor")
            .unwrap()
            .add_nodes_to_group(vec!["lorem"], "dolor")
            .unwrap()
            .remove_nodes_from_group(vec!["lorem"], "dolor")
            .unwrap();

        assert!(
            graphrecord
                .remove_nodes_from_group(vec!["lorem"], "dolor")
                .is_err_and(|error| matches!(error, GraphRecordError::NodeNotInGroup { .. }))
        );
    }

    #[test]
    fn test_add_edges_to_group() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .add_group("dolor")
            .unwrap()
            .add_edges_to_group(vec![edge_index], "dolor")
            .unwrap();

        let state = graphrecord.state();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let group_address = state.resolve_group_address("dolor").unwrap();

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
            .add_group("dolor")
            .unwrap()
            .add_edges_to_group(vec![edge_index], "dolor")
            .unwrap();

        assert!(
            graphrecord
                .add_edges_to_group(vec![edge_index], "dolor")
                .is_err_and(|error| matches!(error, GraphRecordError::EdgeAlreadyInGroup { .. }))
        );
    }

    #[test]
    fn test_remove_edges_from_group() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let graphrecord = graphrecord
            .add_group("dolor")
            .unwrap()
            .add_edges_to_group(vec![edge_index], "dolor")
            .unwrap()
            .remove_edges_from_group(vec![edge_index], "dolor")
            .unwrap();

        let state = graphrecord.state();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let group_address = state.resolve_group_address("dolor").unwrap();

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
            .add_group("dolor")
            .unwrap()
            .add_edges_to_group(vec![edge_index], "dolor")
            .unwrap()
            .remove_edges_from_group(vec![edge_index], "dolor")
            .unwrap();

        assert!(
            graphrecord
                .remove_edges_from_group(vec![edge_index], "dolor")
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
            .set_node_attributes(vec!["ipsum"], create_lorem_attributes())
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

        let (graphrecord, _) = create_graphrecord_with_one_edge();
        let frozen = graphrecord.freeze_schema().unwrap();
        let extended = frozen
            .add_edge("ipsum", "lorem", create_lorem_attributes())
            .unwrap();

        assert_eq!(2, extended.edge_count());
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
            .add_node("dolor", AttributeMap::new())
            .unwrap()
            .remove_nodes(vec!["dolor"])
            .unwrap();

        let compacted = removable.compact();

        assert_eq!(2, compacted.node_count());
        assert_eq!(1, compacted.edge_count());
        assert!(!compacted.contains_edge(&edge_index));
        assert_eq!(
            Some(42.into()),
            compacted
                .node("lorem")
                .unwrap()
                .attribute("lorem")
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
        let graphrecord = GraphRecord::new().add_group("lorem").unwrap();

        assert_eq!(1, graphrecord.group_count());
    }

    #[test]
    fn test_contains_node() {
        let graphrecord = create_graphrecord_with_two_nodes();

        assert!(graphrecord.contains_node("lorem"));
        assert!(!graphrecord.contains_node("dolor"));
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
        let graphrecord = GraphRecord::new().add_group("lorem").unwrap();

        assert!(graphrecord.contains_group("lorem"));
        assert!(!graphrecord.contains_group("ipsum"));
    }

    #[test]
    fn test_node_attribute() {
        let graphrecord = create_graphrecord_with_two_nodes();

        assert_eq!(
            Some(42.into()),
            graphrecord
                .node("lorem")
                .unwrap()
                .attribute("lorem")
                .map(Value::from)
        );
    }

    #[test]
    fn test_edge_attribute() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        assert_eq!(
            Some(42.into()),
            graphrecord
                .edge(&edge_index)
                .unwrap()
                .attribute("lorem")
                .map(Value::from)
        );
    }

    #[test]
    fn test_eq() {
        let graphrecord = create_graphrecord_with_two_nodes();

        assert_eq!(graphrecord, graphrecord.clone());

        let reordered = GraphRecord::new()
            .add_node("ipsum", AttributeMap::new())
            .unwrap()
            .add_node("lorem", create_lorem_attributes())
            .unwrap();

        assert_eq!(graphrecord, reordered);

        let reattributed = reordered
            .set_node_attributes(
                vec!["ipsum"],
                AttributeMap::from([("sed".into(), 1.into())]),
            )
            .unwrap();

        assert_ne!(graphrecord, reattributed);

        let frozen = graphrecord.freeze_schema().unwrap();

        assert_ne!(graphrecord, frozen);

        let (with_edge, edge_index) = create_graphrecord_with_one_edge();
        let sibling = with_edge.add_group("amet").unwrap();
        let twin = with_edge.add_group("amet").unwrap();

        assert_eq!(twin, sibling);
        assert_ne!(with_edge, sibling);

        let node_grouped = sibling.add_nodes_to_group(vec!["lorem"], "amet").unwrap();

        assert_ne!(sibling, node_grouped);

        let edge_grouped = twin.add_edges_to_group(vec![edge_index], "amet").unwrap();

        assert_ne!(twin, edge_grouped);

        let edge_reattributed = with_edge
            .set_edge_attributes(
                vec![edge_index],
                AttributeMap::from([("sed".into(), 1.into())]),
            )
            .unwrap();

        assert_ne!(with_edge, edge_reattributed);
    }

    #[test]
    #[cfg(feature = "plugins")]
    fn test_eq_plugins() {
        use crate::graphrecord::Plugin;

        struct Quiet;

        impl Plugin for Quiet {}

        let graphrecord = create_graphrecord_with_two_nodes();
        let with_plugin = GraphRecord::new()
            .add_node("lorem", create_lorem_attributes())
            .unwrap()
            .add_node("ipsum", AttributeMap::new())
            .unwrap()
            .add_plugin("sed", Quiet)
            .unwrap();

        assert_eq!(graphrecord, with_plugin);
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
