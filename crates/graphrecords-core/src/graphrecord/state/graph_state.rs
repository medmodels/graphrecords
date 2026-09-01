use super::{
    AttributeAddress, EdgeAddress, EdgeEpoch, GroupAddress, NodeAddress, StateIdentity,
    adjacency_chunk::{AdjacencyChunk, AdjacencyEntry},
    attribute_directory::AttributeDirectory,
    chunk_tree::ChunkTree,
    dictionary::KeyDictionary,
    endpoint_chunk::{EdgeEndpoints, EndpointChunk},
    group_directory::GroupDirectory,
    key_chunk::KeyChunk,
    membership_chunk::MembershipChunk,
};
use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        AttributeMap,
        datatypes::{
            AttributeName, AttributeNameView, EdgeIndex, GroupIndex, GroupIndexView, Identifier,
            IdentifierView, NodeIndex, NodeIndexView, Value, ValueView,
        },
        schema::{GroupSchema, Schema, SchemaType},
    },
};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct GraphState {
    #[cfg_attr(
        any(feature = "serde", feature = "io"),
        serde(skip, default = "StateIdentity::mint")
    )]
    pub(crate) identity: StateIdentity,
    pub(crate) schema: Arc<Schema>,
    #[cfg_attr(any(feature = "serde", feature = "io"), serde(skip))]
    pub(crate) node_dictionary: KeyDictionary,
    pub(crate) node_keys: ChunkTree<KeyChunk>,
    pub(crate) node_attributes: AttributeDirectory,
    pub(crate) node_memberships: ChunkTree<MembershipChunk>,
    pub(crate) adjacency_outgoing: ChunkTree<AdjacencyChunk>,
    pub(crate) adjacency_incoming: ChunkTree<AdjacencyChunk>,
    pub(crate) edge_endpoints: ChunkTree<EndpointChunk>,
    pub(crate) edge_attributes: AttributeDirectory,
    pub(crate) edge_memberships: ChunkTree<MembershipChunk>,
    pub(crate) groups: GroupDirectory,
    pub(crate) edge_epochs: Arc<Vec<EdgeEpoch>>,
    pub(crate) next_node_address: NodeAddress,
    pub(crate) next_edge_address: EdgeAddress,
    pub(crate) node_count: usize,
    pub(crate) edge_count: usize,
}

impl GraphState {
    pub fn new() -> Self {
        Self {
            identity: StateIdentity::mint(),
            schema: Arc::new(Schema::default()),
            node_dictionary: KeyDictionary::new(),
            node_keys: ChunkTree::new(),
            node_attributes: AttributeDirectory::new(),
            node_memberships: ChunkTree::new(),
            adjacency_outgoing: ChunkTree::new(),
            adjacency_incoming: ChunkTree::new(),
            edge_endpoints: ChunkTree::new(),
            edge_attributes: AttributeDirectory::new(),
            edge_memberships: ChunkTree::new(),
            groups: GroupDirectory::new(),
            edge_epochs: Arc::new(Vec::new()),
            next_node_address: NodeAddress::new(0),
            next_edge_address: EdgeAddress::new(0),
            node_count: 0,
            edge_count: 0,
        }
    }

    #[cfg(feature = "serde")]
    pub(crate) fn rebuild_dictionaries(&mut self) {
        let mut hashed_addresses = Vec::new();

        for (chunk_index, chunk) in self.node_keys.chunks() {
            for (chunk_local_address, identifier) in chunk.iter() {
                let address = NodeAddress::from_chunk_parts(chunk_index, chunk_local_address);
                let hash = self.node_dictionary.hash_one(identifier);

                hashed_addresses.push((hash, address.index()));
            }
        }

        for (hash, address_index) in hashed_addresses {
            self.node_dictionary.insert(hash, address_index);
        }

        self.groups.rebuild_indices();
    }

    #[cfg(feature = "serde")]
    pub(crate) fn is_referentially_consistent(&self) -> bool {
        let edges_consistent = self.edge_addresses().all(|address| {
            self.edge_index(address).is_some()
                && self.edge_endpoints(address).is_some_and(|endpoints| {
                    self.contains_node_address(endpoints.source_address)
                        && self.contains_node_address(endpoints.target_address)
                })
        });

        let node_memberships_consistent = self
            .node_addresses()
            .flat_map(|address| self.node_memberships(address))
            .all(|group_address| self.group_index(group_address).is_some());

        let edge_memberships_consistent = self
            .edge_addresses()
            .flat_map(|address| self.edge_memberships(address))
            .all(|group_address| self.group_index(group_address).is_some());

        let group_members_consistent = self.group_addresses().all(|group_address| {
            self.groups.record(group_address).is_some_and(|record| {
                record
                    .node_members
                    .iter()
                    .all(|index| self.contains_node_address(NodeAddress::new(index)))
                    && record
                        .edge_members
                        .iter()
                        .all(|index| self.contains_edge_address(EdgeAddress::new(index)))
            })
        });

        edges_consistent
            && node_memberships_consistent
            && edge_memberships_consistent
            && group_members_consistent
    }

    pub const fn identity(&self) -> StateIdentity {
        self.identity
    }

    pub const fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub const fn node_count(&self) -> usize {
        self.node_count
    }

    pub const fn edge_count(&self) -> usize {
        self.edge_count
    }

    pub const fn group_count(&self) -> usize {
        self.groups.group_count()
    }

    pub fn contains_node_address(&self, address: NodeAddress) -> bool {
        self.node_key(address).is_some()
    }

    pub fn contains_edge_address(&self, address: EdgeAddress) -> bool {
        self.edge_endpoints(address).is_some()
    }

    pub fn resolve_node_address<'a>(
        &self,
        node_index: impl Into<NodeIndexView<'a>>,
    ) -> Option<NodeAddress> {
        let node_index = node_index.into();
        let hash = self.node_dictionary.hash_one(node_index.identifier_view());

        self.node_dictionary
            .candidates(hash)
            .find_map(|candidate_index| {
                let candidate_address = NodeAddress::new(candidate_index);

                (self.node_key(candidate_address)? == *node_index.identifier_view())
                    .then_some(candidate_address)
            })
    }

    pub fn node_key(&self, address: NodeAddress) -> Option<IdentifierView<'_>> {
        self.node_keys
            .get(address.chunk_index())?
            .get(address.chunk_local_address())
    }

    pub fn node_attribute(
        &self,
        address: NodeAddress,
        attribute_address: AttributeAddress,
    ) -> Option<ValueView<'_>> {
        self.node_attributes
            .chunk_tree(attribute_address)?
            .get(address.chunk_index())?
            .get(address.chunk_local_address())
    }

    pub fn node_attribute_by_name(
        &self,
        address: NodeAddress,
        attribute_name: &AttributeName,
    ) -> Option<ValueView<'_>> {
        let attribute_address = self.node_attributes.resolve(attribute_name)?;

        self.node_attribute(address, attribute_address)
    }

    pub fn edge_attribute(
        &self,
        address: EdgeAddress,
        attribute_address: AttributeAddress,
    ) -> Option<ValueView<'_>> {
        self.edge_attributes
            .chunk_tree(attribute_address)?
            .get(address.chunk_index())?
            .get(address.chunk_local_address())
    }

    pub fn edge_attribute_by_name(
        &self,
        address: EdgeAddress,
        attribute_name: &AttributeName,
    ) -> Option<ValueView<'_>> {
        let attribute_address = self.edge_attributes.resolve(attribute_name)?;

        self.edge_attribute(address, attribute_address)
    }

    pub fn node_memberships(
        &self,
        address: NodeAddress,
    ) -> impl Iterator<Item = GroupAddress> + '_ {
        self.node_memberships
            .get(address.chunk_index())
            .into_iter()
            .flat_map(move |chunk| chunk.memberships(address.chunk_local_address()))
    }

    pub fn edge_memberships(
        &self,
        address: EdgeAddress,
    ) -> impl Iterator<Item = GroupAddress> + '_ {
        self.edge_memberships
            .get(address.chunk_index())
            .into_iter()
            .flat_map(move |chunk| chunk.memberships(address.chunk_local_address()))
    }

    pub fn node_attribute_entries(
        &self,
    ) -> impl Iterator<Item = (AttributeAddress, &AttributeName)> + '_ {
        self.node_attributes
            .iter()
            .map(|(address, name, _)| (address, name))
    }

    pub fn edge_attribute_entries(
        &self,
    ) -> impl Iterator<Item = (AttributeAddress, &AttributeName)> + '_ {
        self.edge_attributes
            .iter()
            .map(|(address, name, _)| (address, name))
    }

    pub fn resolve_node_attribute_address<'a>(
        &self,
        name: impl Into<AttributeNameView<'a>>,
    ) -> Option<AttributeAddress> {
        self.node_attributes.resolve(name)
    }

    pub fn resolve_edge_attribute_address<'a>(
        &self,
        name: impl Into<AttributeNameView<'a>>,
    ) -> Option<AttributeAddress> {
        self.edge_attributes.resolve(name)
    }

    pub fn node_attribute_name(&self, address: AttributeAddress) -> Option<&AttributeName> {
        self.node_attributes.name(address)
    }

    pub fn edge_attribute_name(&self, address: AttributeAddress) -> Option<&AttributeName> {
        self.edge_attributes.name(address)
    }

    pub fn node_addresses(&self) -> impl Iterator<Item = NodeAddress> + '_ {
        self.node_keys.chunks().flat_map(|(chunk_index, chunk)| {
            chunk.iter().map(move |(chunk_local_address, _)| {
                NodeAddress::from_chunk_parts(chunk_index, chunk_local_address)
            })
        })
    }

    pub fn edge_addresses(&self) -> impl Iterator<Item = EdgeAddress> + '_ {
        self.edge_endpoints
            .chunks()
            .flat_map(|(chunk_index, chunk)| {
                chunk.iter().map(move |(chunk_local_address, _)| {
                    EdgeAddress::from_chunk_parts(chunk_index, chunk_local_address)
                })
            })
    }

    pub fn edge_endpoints(&self, address: EdgeAddress) -> Option<EdgeEndpoints> {
        self.edge_endpoints
            .get(address.chunk_index())?
            .get(address.chunk_local_address())
            .copied()
    }

    pub fn outgoing_edge_addresses(
        &self,
        address: NodeAddress,
    ) -> impl Iterator<Item = EdgeAddress> + '_ {
        Self::adjacency_entries(&self.adjacency_outgoing, address).map(|entry| entry.edge_address)
    }

    pub fn incoming_edge_addresses(
        &self,
        address: NodeAddress,
    ) -> impl Iterator<Item = EdgeAddress> + '_ {
        Self::adjacency_entries(&self.adjacency_incoming, address).map(|entry| entry.edge_address)
    }

    pub fn outgoing_neighbor_addresses(
        &self,
        address: NodeAddress,
    ) -> impl Iterator<Item = NodeAddress> + '_ {
        Self::adjacency_entries(&self.adjacency_outgoing, address)
            .map(|entry| entry.neighbor_address)
    }

    pub fn incoming_neighbor_addresses(
        &self,
        address: NodeAddress,
    ) -> impl Iterator<Item = NodeAddress> + '_ {
        Self::adjacency_entries(&self.adjacency_incoming, address)
            .map(|entry| entry.neighbor_address)
    }

    pub fn neighbor_addresses(
        &self,
        address: NodeAddress,
    ) -> impl Iterator<Item = NodeAddress> + '_ {
        self.outgoing_neighbor_addresses(address)
            .chain(self.incoming_neighbor_addresses(address))
    }

    pub fn edge_index(&self, address: EdgeAddress) -> Option<EdgeIndex> {
        let epoch_position = self
            .edge_epochs
            .partition_point(|epoch| epoch.first_address().index() <= address.index())
            .checked_sub(1)?;

        let epoch = &self.edge_epochs[epoch_position];
        let offset = address.index() - epoch.first_address().index();

        (offset < epoch.edge_count() && self.contains_edge_address(address))
            .then_some(EdgeIndex::new(epoch.tag(), offset))
    }

    pub fn resolve_edge_address(&self, edge_index: &EdgeIndex) -> Option<EdgeAddress> {
        let epoch = self
            .edge_epochs
            .iter()
            .find(|candidate| candidate.tag() == edge_index.tag())?;

        if edge_index.offset() >= epoch.edge_count() {
            return None;
        }

        let address = EdgeAddress::new(epoch.first_address().index() + edge_index.offset());

        self.contains_edge_address(address).then_some(address)
    }

    pub fn resolve_group_address<'a>(
        &self,
        group_index: impl Into<GroupIndexView<'a>>,
    ) -> Option<GroupAddress> {
        self.groups.resolve(group_index)
    }

    pub fn group_index(&self, address: GroupAddress) -> Option<&GroupIndex> {
        self.groups.group_index(address)
    }

    pub fn group_addresses(&self) -> impl Iterator<Item = GroupAddress> + '_ {
        self.groups.iter().map(|(address, _)| address)
    }

    pub fn group_node_member_addresses(
        &self,
        address: GroupAddress,
    ) -> impl Iterator<Item = NodeAddress> + '_ {
        self.groups
            .record(address)
            .into_iter()
            .flat_map(|group_record| group_record.node_members.iter().map(NodeAddress::new))
    }

    pub fn group_edge_member_addresses(
        &self,
        address: GroupAddress,
    ) -> impl Iterator<Item = EdgeAddress> + '_ {
        self.groups
            .record(address)
            .into_iter()
            .flat_map(|group_record| group_record.edge_members.iter().map(EdgeAddress::new))
    }

    pub fn group_node_member_count(&self, address: GroupAddress) -> Option<usize> {
        self.groups
            .record(address)
            .map(|group_record| group_record.node_members.len())
    }

    pub fn group_edge_member_count(&self, address: GroupAddress) -> Option<usize> {
        self.groups
            .record(address)
            .map(|group_record| group_record.edge_members.len())
    }

    fn adjacency_entries(
        chunk_tree: &ChunkTree<AdjacencyChunk>,
        address: NodeAddress,
    ) -> impl Iterator<Item = &AdjacencyEntry> {
        chunk_tree
            .get(address.chunk_index())
            .into_iter()
            .flat_map(move |chunk| chunk.entries(address.chunk_local_address()))
    }

    pub(crate) fn insert_node(
        &mut self,
        key: &NodeIndex,
        attributes: &AttributeMap,
        group_addresses: &[GroupAddress],
    ) -> GraphRecordResult<NodeAddress> {
        if self.resolve_node_address(key).is_some() {
            return Err(GraphRecordError::NodeAlreadyExists {
                node_index: key.clone(),
            });
        }

        if self.next_node_address.index() == u32::MAX {
            return Err(GraphRecordError::AddressSpaceExhausted);
        }

        let address = self.next_node_address;
        self.next_node_address = NodeAddress::new(address.index() + 1);

        self.node_dictionary.insert(
            self.node_dictionary
                .hash_one(IdentifierView::from(key.identifier())),
            address.index(),
        );

        self.node_keys
            .get_mut_or_default(address.chunk_index())
            .insert(address.chunk_local_address(), key.identifier());

        for (name, value) in attributes {
            self.node_attributes.set(name, address.index(), value);
        }

        self.node_count += 1;

        if group_addresses.is_empty() {
            let ungrouped_population_was_empty = self.groups.ungrouped_node_count() == 0;
            self.groups.increment_ungrouped_node_count();

            self.schema_transition_node(address, None, attributes, ungrouped_population_was_empty)?;

            return Ok(address);
        }

        for group_address in group_addresses {
            self.insert_node_membership(address, *group_address, attributes)?;
        }

        Ok(address)
    }

    pub(crate) fn remove_node(&mut self, address: NodeAddress) {
        let key = Identifier::from(self.node_key(address).expect("Node must exist."));

        let incident_edge_addresses: GrHashSet<_> = self
            .outgoing_edge_addresses(address)
            .chain(self.incoming_edge_addresses(address))
            .collect();

        for edge_address in incident_edge_addresses {
            self.remove_edge(edge_address);
        }

        let node_was_previously_ungrouped = self.node_memberships(address).next().is_none();
        let group_addresses: Vec<_> = self.node_memberships(address).collect();

        for group_address in group_addresses {
            self.remove_node_membership(address, group_address);
        }

        if node_was_previously_ungrouped {
            self.groups.decrement_ungrouped_node_count();
        }

        self.node_dictionary.remove(
            self.node_dictionary.hash_one(IdentifierView::from(&key)),
            address.index(),
        );

        let key_chunk = self
            .node_keys
            .get_mut(address.chunk_index())
            .expect("Chunk must exist.");
        key_chunk.remove(address.chunk_local_address());
        if key_chunk.is_empty() {
            self.node_keys.remove_chunk(address.chunk_index());
        }

        let attribute_addresses: Vec<_> = self
            .node_attribute_entries()
            .map(|(attribute_address, _)| attribute_address)
            .collect();

        for attribute_address in attribute_addresses {
            self.node_attributes
                .remove_value(attribute_address, address.index());
        }
        self.node_attributes.prune_empty();

        if let Some(outgoing_chunk) = self.adjacency_outgoing.get_mut(address.chunk_index()) {
            outgoing_chunk.remove_cell(address.chunk_local_address());
            if outgoing_chunk.is_empty() {
                self.adjacency_outgoing.remove_chunk(address.chunk_index());
            }
        }

        if let Some(incoming_chunk) = self.adjacency_incoming.get_mut(address.chunk_index()) {
            incoming_chunk.remove_cell(address.chunk_local_address());
            if incoming_chunk.is_empty() {
                self.adjacency_incoming.remove_chunk(address.chunk_index());
            }
        }

        self.node_count -= 1;
    }

    pub(crate) fn insert_edges(
        &mut self,
        resolved_edges: Vec<(NodeAddress, NodeAddress, &AttributeMap)>,
        group_addresses: &[GroupAddress],
    ) -> GraphRecordResult<()> {
        let first_address = self.next_edge_address;
        let edge_count = u32::try_from(resolved_edges.len())
            .map_err(|_| GraphRecordError::AddressSpaceExhausted)?;

        self.append_edge_epoch(first_address, edge_count);

        for (source_address, target_address, attributes) in resolved_edges {
            self.insert_edge(source_address, target_address, attributes, group_addresses)?;
        }

        Ok(())
    }

    fn insert_edge(
        &mut self,
        source_address: NodeAddress,
        target_address: NodeAddress,
        attributes: &AttributeMap,
        group_addresses: &[GroupAddress],
    ) -> GraphRecordResult<EdgeAddress> {
        if self.next_edge_address.index() == u32::MAX {
            return Err(GraphRecordError::AddressSpaceExhausted);
        }

        let address = self.next_edge_address;
        self.next_edge_address = EdgeAddress::new(address.index() + 1);

        self.edge_endpoints
            .get_mut_or_default(address.chunk_index())
            .set(
                address.chunk_local_address(),
                EdgeEndpoints {
                    source_address,
                    target_address,
                },
            );

        self.adjacency_outgoing
            .get_mut_or_default(source_address.chunk_index())
            .add(
                source_address.chunk_local_address(),
                AdjacencyEntry {
                    neighbor_address: target_address,
                    edge_address: address,
                },
            );

        self.adjacency_incoming
            .get_mut_or_default(target_address.chunk_index())
            .add(
                target_address.chunk_local_address(),
                AdjacencyEntry {
                    neighbor_address: source_address,
                    edge_address: address,
                },
            );

        for (name, value) in attributes {
            self.edge_attributes.set(name, address.index(), value);
        }

        self.edge_count += 1;

        if group_addresses.is_empty() {
            let ungrouped_population_was_empty = self.groups.ungrouped_edge_count() == 0;
            self.groups.increment_ungrouped_edge_count();

            self.schema_transition_edge(address, None, attributes, ungrouped_population_was_empty)?;

            return Ok(address);
        }

        for group_address in group_addresses {
            self.insert_edge_membership(address, *group_address, attributes)?;
        }

        Ok(address)
    }

    pub(crate) fn remove_edge(&mut self, address: EdgeAddress) {
        let endpoints = self.edge_endpoints(address).expect("Edge must exist.");

        let outgoing_chunk = self
            .adjacency_outgoing
            .get_mut(endpoints.source_address.chunk_index())
            .expect("Chunk must exist.");
        outgoing_chunk.remove(endpoints.source_address.chunk_local_address(), address);
        if outgoing_chunk.is_empty() {
            self.adjacency_outgoing
                .remove_chunk(endpoints.source_address.chunk_index());
        }

        let incoming_chunk = self
            .adjacency_incoming
            .get_mut(endpoints.target_address.chunk_index())
            .expect("Chunk must exist.");
        incoming_chunk.remove(endpoints.target_address.chunk_local_address(), address);
        if incoming_chunk.is_empty() {
            self.adjacency_incoming
                .remove_chunk(endpoints.target_address.chunk_index());
        }

        let edge_was_previously_ungrouped = self.edge_memberships(address).next().is_none();
        let group_addresses: Vec<_> = self.edge_memberships(address).collect();

        for group_address in group_addresses {
            self.remove_edge_membership(address, group_address);
        }

        if edge_was_previously_ungrouped {
            self.groups.decrement_ungrouped_edge_count();
        }

        let attribute_addresses: Vec<_> = self
            .edge_attribute_entries()
            .map(|(attribute_address, _)| attribute_address)
            .collect();

        for attribute_address in attribute_addresses {
            self.edge_attributes
                .remove_value(attribute_address, address.index());
        }
        self.edge_attributes.prune_empty();

        let endpoint_chunk = self
            .edge_endpoints
            .get_mut(address.chunk_index())
            .expect("Chunk must exist.");
        endpoint_chunk.remove(address.chunk_local_address());
        if endpoint_chunk.is_empty() {
            self.edge_endpoints.remove_chunk(address.chunk_index());
        }

        self.edge_count -= 1;
    }

    pub(crate) fn add_node_to_group(
        &mut self,
        node_address: NodeAddress,
        group_address: GroupAddress,
    ) -> GraphRecordResult<()> {
        if self
            .node_memberships(node_address)
            .any(|candidate| candidate == group_address)
        {
            let node_index = NodeIndex::from(Identifier::from(
                self.node_key(node_address).expect("Node must exist."),
            ));
            let group_index = self
                .group_index(group_address)
                .expect("Group must exist.")
                .clone();

            return Err(GraphRecordError::NodeAlreadyInGroup {
                node_index,
                group_index,
            });
        }

        let node_was_previously_ungrouped = self.node_memberships(node_address).next().is_none();

        self.node_memberships
            .get_mut_or_default(node_address.chunk_index())
            .add(node_address.chunk_local_address(), group_address);

        if node_was_previously_ungrouped {
            self.groups.decrement_ungrouped_node_count();
        }

        let group_population_was_empty = {
            let group_record = self
                .groups
                .record_mut(group_address)
                .expect("Group must exist.");
            let was_empty = group_record.node_members.is_empty();
            group_record.node_members.insert(node_address.index());

            was_empty
        };

        let group_index = self
            .group_index(group_address)
            .expect("Group must exist.")
            .clone();
        let attributes = self.node_attribute_map(node_address);

        self.schema_transition_node(
            node_address,
            Some(&group_index),
            &attributes,
            group_population_was_empty,
        )
    }

    pub(crate) fn remove_node_from_group(
        &mut self,
        node_address: NodeAddress,
        group_address: GroupAddress,
    ) -> GraphRecordResult<()> {
        if !self.remove_node_membership(node_address, group_address) {
            let node_index = NodeIndex::from(Identifier::from(
                self.node_key(node_address).expect("Node must exist."),
            ));
            let group_index = self
                .group_index(group_address)
                .expect("Group must exist.")
                .clone();

            return Err(GraphRecordError::NodeNotInGroup {
                node_index,
                group_index,
            });
        }

        self.transition_node_to_ungrouped_if_memberless(node_address)
    }

    pub(crate) fn add_edge_to_group(
        &mut self,
        edge_address: EdgeAddress,
        group_address: GroupAddress,
    ) -> GraphRecordResult<()> {
        if self
            .edge_memberships(edge_address)
            .any(|candidate| candidate == group_address)
        {
            let group_index = self
                .group_index(group_address)
                .expect("Group must exist.")
                .clone();

            return Err(GraphRecordError::EdgeAlreadyInGroup {
                edge_index: self
                    .edge_index(edge_address)
                    .expect("Edge must belong to an epoch."),
                group_index,
            });
        }

        let edge_was_previously_ungrouped = self.edge_memberships(edge_address).next().is_none();

        self.edge_memberships
            .get_mut_or_default(edge_address.chunk_index())
            .add(edge_address.chunk_local_address(), group_address);

        if edge_was_previously_ungrouped {
            self.groups.decrement_ungrouped_edge_count();
        }

        let group_population_was_empty = {
            let group_record = self
                .groups
                .record_mut(group_address)
                .expect("Group must exist.");
            let was_empty = group_record.edge_members.is_empty();
            group_record.edge_members.insert(edge_address.index());

            was_empty
        };

        let group_index = self
            .group_index(group_address)
            .expect("Group must exist.")
            .clone();
        let attributes = self.edge_attribute_map(edge_address);

        self.schema_transition_edge(
            edge_address,
            Some(&group_index),
            &attributes,
            group_population_was_empty,
        )
    }

    pub(crate) fn remove_edge_from_group(
        &mut self,
        edge_address: EdgeAddress,
        group_address: GroupAddress,
    ) -> GraphRecordResult<()> {
        if !self.remove_edge_membership(edge_address, group_address) {
            let group_index = self
                .group_index(group_address)
                .expect("Group must exist.")
                .clone();

            return Err(GraphRecordError::EdgeNotInGroup {
                edge_index: self
                    .edge_index(edge_address)
                    .expect("Edge must belong to an epoch."),
                group_index,
            });
        }

        self.transition_edge_to_ungrouped_if_memberless(edge_address)
    }

    pub(crate) fn insert_group(&mut self, name: &GroupIndex) -> GraphRecordResult<GroupAddress> {
        let group_address =
            self.groups
                .add(name.clone())
                .ok_or_else(|| GraphRecordError::GroupAlreadyExists {
                    group_index: name.clone(),
                })?;

        match self.schema.schema_type() {
            SchemaType::Inferred => {
                if !self.schema.groups().contains_key(name) {
                    Arc::make_mut(&mut self.schema)
                        .add_group(name.clone(), GroupSchema::default())
                        .expect("Group must be absent from the schema.");
                }
            }
            SchemaType::Provided => {
                self.schema.group(name)?;
            }
        }

        Ok(group_address)
    }

    pub(crate) fn remove_group(&mut self, group_address: GroupAddress) -> GraphRecordResult<()> {
        let record = self
            .groups
            .remove(group_address)
            .expect("Group must exist.");

        for member_index in record.node_members.iter() {
            let node_address = NodeAddress::new(member_index);

            self.remove_node_membership(node_address, group_address);
            self.transition_node_to_ungrouped_if_memberless(node_address)?;
        }

        for member_index in record.edge_members.iter() {
            let edge_address = EdgeAddress::new(member_index);

            self.remove_edge_membership(edge_address, group_address);
            self.transition_edge_to_ungrouped_if_memberless(edge_address)?;
        }

        Ok(())
    }

    pub(crate) fn set_node_attributes(
        &mut self,
        node_address: NodeAddress,
        attributes: &AttributeMap,
    ) -> GraphRecordResult<()> {
        for (name, value) in attributes {
            self.node_attributes.set(name, node_address.index(), value);
        }

        let post_change_attributes = self.node_attribute_map(node_address);

        self.schema_transition_node_memberships(node_address, &post_change_attributes)
    }

    pub(crate) fn replace_node_attributes(
        &mut self,
        node_address: NodeAddress,
        attributes: &AttributeMap,
    ) -> GraphRecordResult<()> {
        let attribute_addresses_to_clear: Vec<_> = self
            .node_attribute_entries()
            .filter(|(_, name)| !attributes.contains_key(*name))
            .map(|(attribute_address, _)| attribute_address)
            .collect();

        for attribute_address in attribute_addresses_to_clear {
            self.node_attributes
                .remove_value(attribute_address, node_address.index());
        }
        self.node_attributes.prune_empty();

        for (name, value) in attributes {
            self.node_attributes.set(name, node_address.index(), value);
        }

        let post_change_attributes = self.node_attribute_map(node_address);

        self.schema_transition_node_memberships(node_address, &post_change_attributes)
    }

    pub(crate) fn remove_node_attribute(
        &mut self,
        node_address: NodeAddress,
        attribute_name: &AttributeName,
    ) -> GraphRecordResult<()> {
        let removed = self
            .resolve_node_attribute_address(attribute_name)
            .is_some_and(|attribute_address| {
                self.node_attributes
                    .remove_value(attribute_address, node_address.index())
            });

        if !removed {
            let node_index = NodeIndex::from(Identifier::from(
                self.node_key(node_address).expect("Node must exist."),
            ));

            return Err(GraphRecordError::NodeAttributeNotFound {
                node_index,
                attribute_name: attribute_name.clone(),
            });
        }
        self.node_attributes.prune_empty();

        let post_change_attributes = self.node_attribute_map(node_address);

        self.schema_transition_node_memberships(node_address, &post_change_attributes)
    }

    pub(crate) fn set_edge_attributes(
        &mut self,
        edge_address: EdgeAddress,
        attributes: &AttributeMap,
    ) -> GraphRecordResult<()> {
        for (name, value) in attributes {
            self.edge_attributes.set(name, edge_address.index(), value);
        }

        let post_change_attributes = self.edge_attribute_map(edge_address);

        self.schema_transition_edge_memberships(edge_address, &post_change_attributes)
    }

    pub(crate) fn replace_edge_attributes(
        &mut self,
        edge_address: EdgeAddress,
        attributes: &AttributeMap,
    ) -> GraphRecordResult<()> {
        let attribute_addresses_to_clear: Vec<_> = self
            .edge_attribute_entries()
            .filter(|(_, name)| !attributes.contains_key(*name))
            .map(|(attribute_address, _)| attribute_address)
            .collect();

        for attribute_address in attribute_addresses_to_clear {
            self.edge_attributes
                .remove_value(attribute_address, edge_address.index());
        }
        self.edge_attributes.prune_empty();

        for (name, value) in attributes {
            self.edge_attributes.set(name, edge_address.index(), value);
        }

        let post_change_attributes = self.edge_attribute_map(edge_address);

        self.schema_transition_edge_memberships(edge_address, &post_change_attributes)
    }

    pub(crate) fn remove_edge_attribute(
        &mut self,
        edge_address: EdgeAddress,
        attribute_name: &AttributeName,
    ) -> GraphRecordResult<()> {
        let removed = self
            .resolve_edge_attribute_address(attribute_name)
            .is_some_and(|attribute_address| {
                self.edge_attributes
                    .remove_value(attribute_address, edge_address.index())
            });

        if !removed {
            return Err(GraphRecordError::EdgeAttributeNotFound {
                edge_index: self
                    .edge_index(edge_address)
                    .expect("Edge must belong to an epoch."),
                attribute_name: attribute_name.clone(),
            });
        }
        self.edge_attributes.prune_empty();

        let post_change_attributes = self.edge_attribute_map(edge_address);

        self.schema_transition_edge_memberships(edge_address, &post_change_attributes)
    }

    pub(crate) fn node_attribute_map(&self, node_address: NodeAddress) -> AttributeMap {
        self.node_attribute_entries()
            .filter_map(|(attribute_address, name)| {
                self.node_attribute(node_address, attribute_address)
                    .map(|value| (name.clone(), Value::from(value)))
            })
            .collect()
    }

    pub(crate) fn edge_attribute_map(&self, edge_address: EdgeAddress) -> AttributeMap {
        self.edge_attribute_entries()
            .filter_map(|(attribute_address, name)| {
                self.edge_attribute(edge_address, attribute_address)
                    .map(|value| (name.clone(), Value::from(value)))
            })
            .collect()
    }

    pub(crate) fn freeze_schema(&mut self) {
        Arc::make_mut(&mut self.schema).freeze();
    }

    pub(crate) fn unfreeze_schema(&mut self) {
        Arc::make_mut(&mut self.schema).unfreeze();
    }

    pub(crate) fn replace_schema(&mut self, schema: Arc<Schema>) {
        self.schema = schema;
    }

    pub(crate) fn clear_content(&mut self) {
        self.node_dictionary = KeyDictionary::new();
        self.node_keys = ChunkTree::new();
        self.node_attributes = AttributeDirectory::new();
        self.node_memberships = ChunkTree::new();
        self.adjacency_outgoing = ChunkTree::new();
        self.adjacency_incoming = ChunkTree::new();
        self.edge_endpoints = ChunkTree::new();
        self.edge_attributes = AttributeDirectory::new();
        self.edge_memberships = ChunkTree::new();
        self.groups = GroupDirectory::new();
        self.edge_epochs = Arc::new(Vec::new());
        self.next_node_address = NodeAddress::new(0);
        self.next_edge_address = EdgeAddress::new(0);
        self.node_count = 0;
        self.edge_count = 0;
    }

    pub(crate) fn compact(&mut self) {
        let old_node_addresses: Vec<_> = self.node_addresses().collect();
        let old_edge_addresses: Vec<_> = self.edge_addresses().collect();
        let old_group_addresses: Vec<_> = self.group_addresses().collect();

        let node_renumbering: GrHashMap<_, _> = old_node_addresses
            .iter()
            .enumerate()
            .map(|(new_index, old_address)| (old_address.index(), new_index as u32))
            .collect();
        let edge_renumbering: GrHashMap<_, _> = old_edge_addresses
            .iter()
            .enumerate()
            .map(|(new_index, old_address)| (old_address.index(), new_index as u32))
            .collect();
        let group_renumbering: GrHashMap<_, _> = old_group_addresses
            .iter()
            .enumerate()
            .map(|(new_index, old_address)| (old_address.index(), new_index as u32))
            .collect();

        let mut node_dictionary = KeyDictionary::new();
        let mut node_keys: ChunkTree<KeyChunk> = ChunkTree::new();
        let mut node_attributes = AttributeDirectory::new();
        let mut node_memberships: ChunkTree<MembershipChunk> = ChunkTree::new();
        let mut adjacency_outgoing: ChunkTree<AdjacencyChunk> = ChunkTree::new();
        let mut adjacency_incoming: ChunkTree<AdjacencyChunk> = ChunkTree::new();
        let mut edge_endpoints: ChunkTree<EndpointChunk> = ChunkTree::new();
        let mut edge_attributes = AttributeDirectory::new();
        let mut edge_memberships: ChunkTree<MembershipChunk> = ChunkTree::new();
        let mut groups = GroupDirectory::new();
        let mut ungrouped_node_count = 0_usize;
        let mut ungrouped_edge_count = 0_usize;

        for (new_index, &old_node_address) in old_node_addresses.iter().enumerate() {
            let new_node_address = NodeAddress::new(new_index as u32);
            let identifier =
                Identifier::from(self.node_key(old_node_address).expect("Node must exist."));
            let hash = node_dictionary.hash_one(IdentifierView::from(&identifier));

            node_dictionary.insert(hash, new_node_address.index());
            node_keys
                .get_mut_or_default(new_node_address.chunk_index())
                .insert(new_node_address.chunk_local_address(), &identifier);

            for (attribute_address, name) in self.node_attribute_entries() {
                if let Some(value) = self.node_attribute(old_node_address, attribute_address) {
                    node_attributes.set(name, new_node_address.index(), &Value::from(value));
                }
            }

            let mut has_membership = false;
            for old_group_address in self.node_memberships(old_node_address) {
                has_membership = true;

                let new_group_address =
                    GroupAddress::new(group_renumbering[&old_group_address.index()]);
                node_memberships
                    .get_mut_or_default(new_node_address.chunk_index())
                    .add(new_node_address.chunk_local_address(), new_group_address);
            }

            if !has_membership {
                ungrouped_node_count += 1;
            }
        }

        for (new_index, &old_edge_address) in old_edge_addresses.iter().enumerate() {
            let new_edge_address = EdgeAddress::new(new_index as u32);
            let old_endpoints = self
                .edge_endpoints(old_edge_address)
                .expect("Edge must exist.");

            let new_source_address =
                NodeAddress::new(node_renumbering[&old_endpoints.source_address.index()]);
            let new_target_address =
                NodeAddress::new(node_renumbering[&old_endpoints.target_address.index()]);

            edge_endpoints
                .get_mut_or_default(new_edge_address.chunk_index())
                .set(
                    new_edge_address.chunk_local_address(),
                    EdgeEndpoints {
                        source_address: new_source_address,
                        target_address: new_target_address,
                    },
                );

            adjacency_outgoing
                .get_mut_or_default(new_source_address.chunk_index())
                .add(
                    new_source_address.chunk_local_address(),
                    AdjacencyEntry {
                        neighbor_address: new_target_address,
                        edge_address: new_edge_address,
                    },
                );

            adjacency_incoming
                .get_mut_or_default(new_target_address.chunk_index())
                .add(
                    new_target_address.chunk_local_address(),
                    AdjacencyEntry {
                        neighbor_address: new_source_address,
                        edge_address: new_edge_address,
                    },
                );

            for (attribute_address, name) in self.edge_attribute_entries() {
                if let Some(value) = self.edge_attribute(old_edge_address, attribute_address) {
                    edge_attributes.set(name, new_edge_address.index(), &Value::from(value));
                }
            }

            let mut has_membership = false;
            for old_group_address in self.edge_memberships(old_edge_address) {
                has_membership = true;

                let new_group_address =
                    GroupAddress::new(group_renumbering[&old_group_address.index()]);
                edge_memberships
                    .get_mut_or_default(new_edge_address.chunk_index())
                    .add(new_edge_address.chunk_local_address(), new_group_address);
            }

            if !has_membership {
                ungrouped_edge_count += 1;
            }
        }

        for old_group_address in old_group_addresses.iter().copied() {
            let record = self
                .groups
                .record(old_group_address)
                .expect("Group must exist.");
            let new_group_address = groups
                .add(record.group_index.clone())
                .expect("Group names must remain unique.");

            let group_record = groups
                .record_mut(new_group_address)
                .expect("Group must exist.");

            for old_member_index in record.node_members.iter() {
                group_record
                    .node_members
                    .insert(node_renumbering[&old_member_index]);
            }

            for old_member_index in record.edge_members.iter() {
                group_record
                    .edge_members
                    .insert(edge_renumbering[&old_member_index]);
            }
        }

        for _ in 0..ungrouped_node_count {
            groups.increment_ungrouped_node_count();
        }

        for _ in 0..ungrouped_edge_count {
            groups.increment_ungrouped_edge_count();
        }

        let edge_epochs = if old_edge_addresses.is_empty() {
            Arc::new(Vec::new())
        } else {
            Arc::new(vec![EdgeEpoch::mint(
                &[],
                EdgeAddress::new(0),
                old_edge_addresses.len() as u32,
            )])
        };

        self.node_dictionary = node_dictionary;
        self.node_keys = node_keys;
        self.node_attributes = node_attributes;
        self.node_memberships = node_memberships;
        self.adjacency_outgoing = adjacency_outgoing;
        self.adjacency_incoming = adjacency_incoming;
        self.edge_endpoints = edge_endpoints;
        self.edge_attributes = edge_attributes;
        self.edge_memberships = edge_memberships;
        self.next_node_address = NodeAddress::new(old_node_addresses.len() as u32);
        self.next_edge_address = EdgeAddress::new(old_edge_addresses.len() as u32);
        self.node_count = old_node_addresses.len();
        self.edge_count = old_edge_addresses.len();
        self.groups = groups;
        self.edge_epochs = edge_epochs;
    }

    fn append_edge_epoch(&mut self, first_address: EdgeAddress, edge_count: u32) {
        if edge_count == 0 {
            return;
        }

        let epoch = EdgeEpoch::mint(&self.edge_epochs, first_address, edge_count);

        Arc::make_mut(&mut self.edge_epochs).push(epoch);
    }

    fn insert_node_membership(
        &mut self,
        node_address: NodeAddress,
        group_address: GroupAddress,
        attributes: &AttributeMap,
    ) -> GraphRecordResult<()> {
        self.node_memberships
            .get_mut_or_default(node_address.chunk_index())
            .add(node_address.chunk_local_address(), group_address);

        let group_record = self
            .groups
            .record_mut(group_address)
            .expect("Group must exist.");
        let group_population_was_empty = group_record.node_members.is_empty();
        group_record.node_members.insert(node_address.index());

        let group_index = self
            .group_index(group_address)
            .expect("Group must exist.")
            .clone();

        self.schema_transition_node(
            node_address,
            Some(&group_index),
            attributes,
            group_population_was_empty,
        )
    }

    fn insert_edge_membership(
        &mut self,
        edge_address: EdgeAddress,
        group_address: GroupAddress,
        attributes: &AttributeMap,
    ) -> GraphRecordResult<()> {
        self.edge_memberships
            .get_mut_or_default(edge_address.chunk_index())
            .add(edge_address.chunk_local_address(), group_address);

        let group_record = self
            .groups
            .record_mut(group_address)
            .expect("Group must exist.");
        let group_population_was_empty = group_record.edge_members.is_empty();
        group_record.edge_members.insert(edge_address.index());

        let group_index = self
            .group_index(group_address)
            .expect("Group must exist.")
            .clone();

        self.schema_transition_edge(
            edge_address,
            Some(&group_index),
            attributes,
            group_population_was_empty,
        )
    }

    fn remove_node_membership(
        &mut self,
        node_address: NodeAddress,
        group_address: GroupAddress,
    ) -> bool {
        let Some(chunk) = self.node_memberships.get_mut(node_address.chunk_index()) else {
            return false;
        };
        let removed = chunk.remove(node_address.chunk_local_address(), group_address);

        if chunk.is_empty() {
            self.node_memberships
                .remove_chunk(node_address.chunk_index());
        }

        if let Some(group_record) = self.groups.record_mut(group_address) {
            group_record.node_members.remove(node_address.index());
        }

        removed
    }

    fn transition_node_to_ungrouped_if_memberless(
        &mut self,
        node_address: NodeAddress,
    ) -> GraphRecordResult<()> {
        if self.node_memberships(node_address).next().is_some() {
            return Ok(());
        }

        let ungrouped_population_was_empty = self.groups.ungrouped_node_count() == 0;
        self.groups.increment_ungrouped_node_count();

        let attributes = self.node_attribute_map(node_address);

        self.schema_transition_node(
            node_address,
            None,
            &attributes,
            ungrouped_population_was_empty,
        )
    }

    fn remove_edge_membership(
        &mut self,
        edge_address: EdgeAddress,
        group_address: GroupAddress,
    ) -> bool {
        let Some(chunk) = self.edge_memberships.get_mut(edge_address.chunk_index()) else {
            return false;
        };
        let removed = chunk.remove(edge_address.chunk_local_address(), group_address);

        if chunk.is_empty() {
            self.edge_memberships
                .remove_chunk(edge_address.chunk_index());
        }

        if let Some(group_record) = self.groups.record_mut(group_address) {
            group_record.edge_members.remove(edge_address.index());
        }

        removed
    }

    fn transition_edge_to_ungrouped_if_memberless(
        &mut self,
        edge_address: EdgeAddress,
    ) -> GraphRecordResult<()> {
        if self.edge_memberships(edge_address).next().is_some() {
            return Ok(());
        }

        let ungrouped_population_was_empty = self.groups.ungrouped_edge_count() == 0;
        self.groups.increment_ungrouped_edge_count();

        let attributes = self.edge_attribute_map(edge_address);

        self.schema_transition_edge(
            edge_address,
            None,
            &attributes,
            ungrouped_population_was_empty,
        )
    }

    fn schema_transition_node(
        &mut self,
        node_address: NodeAddress,
        group_index: Option<&GroupIndex>,
        attributes: &AttributeMap,
        population_was_empty: bool,
    ) -> GraphRecordResult<()> {
        match self.schema.schema_type() {
            SchemaType::Inferred => {
                Arc::make_mut(&mut self.schema).update_node(
                    attributes,
                    group_index,
                    population_was_empty,
                );

                Ok(())
            }
            SchemaType::Provided => {
                let node_index = NodeIndex::from(Identifier::from(
                    self.node_key(node_address).expect("Node must exist."),
                ));

                self.schema
                    .validate_node(&node_index, attributes, group_index)
                    .map_err(GraphRecordError::from)
            }
        }
    }

    fn schema_transition_node_memberships(
        &mut self,
        node_address: NodeAddress,
        attributes: &AttributeMap,
    ) -> GraphRecordResult<()> {
        let group_indices: Vec<_> = self
            .node_memberships(node_address)
            .filter_map(|group_address| self.group_index(group_address).cloned())
            .collect();

        if group_indices.is_empty() {
            return self.schema_transition_node(node_address, None, attributes, false);
        }

        group_indices.iter().try_for_each(|group_index| {
            self.schema_transition_node(node_address, Some(group_index), attributes, false)
        })
    }

    fn schema_transition_edge(
        &mut self,
        edge_address: EdgeAddress,
        group_index: Option<&GroupIndex>,
        attributes: &AttributeMap,
        population_was_empty: bool,
    ) -> GraphRecordResult<()> {
        match self.schema.schema_type() {
            SchemaType::Inferred => {
                Arc::make_mut(&mut self.schema).update_edge(
                    attributes,
                    group_index,
                    population_was_empty,
                );

                Ok(())
            }
            SchemaType::Provided => {
                let edge_index = self
                    .edge_index(edge_address)
                    .expect("Edge must belong to an epoch.");

                self.schema
                    .validate_edge(&edge_index, attributes, group_index)
                    .map_err(GraphRecordError::from)
            }
        }
    }

    fn schema_transition_edge_memberships(
        &mut self,
        edge_address: EdgeAddress,
        attributes: &AttributeMap,
    ) -> GraphRecordResult<()> {
        let group_indices: Vec<_> = self
            .edge_memberships(edge_address)
            .filter_map(|group_address| self.group_index(group_address).cloned())
            .collect();

        if group_indices.is_empty() {
            return self.schema_transition_edge(edge_address, None, attributes, false);
        }

        group_indices.iter().try_for_each(|group_index| {
            self.schema_transition_edge(edge_address, Some(group_index), attributes, false)
        })
    }
}

impl Default for GraphState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::GraphState;
    use crate::{
        errors::GraphRecordError,
        graphrecord::{
            AttributeMap,
            datatypes::{
                AttributeName, DataType, EdgeIndex, GroupIndex, Identifier, IdentifierView,
                NodeIndex, Value,
            },
            schema::{
                AttributeDataType, AttributeSchema, AttributeType, GroupSchema, Schema, SchemaType,
            },
            state::{
                AttributeAddress, EdgeAddress, EdgeEpoch, GroupAddress, NodeAddress,
                adjacency_chunk::AdjacencyEntry, endpoint_chunk::EdgeEndpoints,
            },
        },
    };
    use std::{collections::HashMap, sync::Arc};

    fn create_state_with_three_nodes() -> (GraphState, [NodeAddress; 3]) {
        let mut state = GraphState::new();
        let mut addresses = [NodeAddress::new(0); 3];

        for (position, key) in ["lorem", "ipsum", "dolor"].into_iter().enumerate() {
            let identifier = Identifier::from(key);
            let hash = state
                .node_dictionary
                .hash_one(IdentifierView::from(&identifier));
            let address = state.next_node_address;

            state.node_dictionary.insert(hash, address.index());

            state
                .node_keys
                .get_mut_or_default(address.chunk_index())
                .insert(address.chunk_local_address(), &identifier);

            state.next_node_address = NodeAddress::new(address.index() + 1);
            state.node_count += 1;
            addresses[position] = address;
        }

        (state, addresses)
    }

    fn create_state_with_one_edge() -> (GraphState, [NodeAddress; 3], EdgeAddress) {
        let (mut state, node_addresses) = create_state_with_three_nodes();
        let edge_address = state.next_edge_address;

        state
            .edge_endpoints
            .get_mut_or_default(edge_address.chunk_index())
            .set(
                edge_address.chunk_local_address(),
                EdgeEndpoints {
                    source_address: node_addresses[0],
                    target_address: node_addresses[1],
                },
            );

        state
            .adjacency_outgoing
            .get_mut_or_default(node_addresses[0].chunk_index())
            .add(
                node_addresses[0].chunk_local_address(),
                AdjacencyEntry {
                    neighbor_address: node_addresses[1],
                    edge_address,
                },
            );

        state
            .adjacency_incoming
            .get_mut_or_default(node_addresses[1].chunk_index())
            .add(
                node_addresses[1].chunk_local_address(),
                AdjacencyEntry {
                    neighbor_address: node_addresses[0],
                    edge_address,
                },
            );

        state.edge_epochs = Arc::new(vec![EdgeEpoch::mint(&[], edge_address, 1)]);
        state.next_edge_address = EdgeAddress::new(edge_address.index() + 1);
        state.edge_count += 1;

        (state, node_addresses, edge_address)
    }

    fn create_state_with_one_raw_group() -> (GraphState, GroupAddress) {
        let mut state = GraphState::new();
        let group_address = state.groups.add(GroupIndex::from("lorem")).unwrap();

        (state, group_address)
    }

    fn create_lorem_attributes() -> AttributeMap {
        AttributeMap::from([("lorem".into(), 42.into())])
    }

    fn create_state_with_two_nodes() -> (GraphState, NodeAddress, NodeAddress) {
        let mut state = GraphState::new();
        let first_address = state
            .insert_node(&NodeIndex::from("lorem"), &create_lorem_attributes(), &[])
            .unwrap();
        let second_address = state
            .insert_node(&NodeIndex::from("ipsum"), &AttributeMap::new(), &[])
            .unwrap();

        (state, first_address, second_address)
    }

    fn create_state_with_one_inserted_edge() -> (GraphState, NodeAddress, NodeAddress, EdgeAddress)
    {
        let (mut state, first_address, second_address) = create_state_with_two_nodes();
        let edge_address = state
            .insert_edge(
                first_address,
                second_address,
                &create_lorem_attributes(),
                &[],
            )
            .unwrap();
        state.append_edge_epoch(edge_address, 1);

        (state, first_address, second_address, edge_address)
    }

    fn create_state_with_one_group() -> (GraphState, GroupAddress) {
        let mut state = GraphState::new();
        let group_address = state.insert_group(&GroupIndex::from("dolor")).unwrap();

        (state, group_address)
    }

    fn create_state_with_one_group_and_one_edge() -> (
        GraphState,
        GroupAddress,
        NodeAddress,
        NodeAddress,
        EdgeAddress,
    ) {
        let (mut state, group_address) = create_state_with_one_group();
        let first_address = state
            .insert_node(&NodeIndex::from("lorem"), &create_lorem_attributes(), &[])
            .unwrap();
        let second_address = state
            .insert_node(&NodeIndex::from("ipsum"), &AttributeMap::new(), &[])
            .unwrap();
        let edge_address = state
            .insert_edge(
                first_address,
                second_address,
                &create_lorem_attributes(),
                &[],
            )
            .unwrap();
        state.append_edge_epoch(edge_address, 1);

        (
            state,
            group_address,
            first_address,
            second_address,
            edge_address,
        )
    }

    fn create_provided_group_schema() -> GroupSchema {
        let attributes = AttributeSchema::new(HashMap::from([(
            AttributeName::from("lorem"),
            AttributeDataType::new(DataType::Int, AttributeType::Continuous).unwrap(),
        )]));

        GroupSchema::new(attributes.clone(), attributes)
    }

    #[test]
    fn test_new() {
        let state = GraphState::new();

        assert_eq!(0, state.node_count());
        assert_eq!(0, state.edge_count());
        assert_eq!(0, state.group_count());
        assert_eq!(0, state.node_addresses().count());
        assert_eq!(0, state.edge_addresses().count());
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_rebuild_dictionaries() {
        let (mut state, addresses) = create_state_with_three_nodes();
        state.node_dictionary = super::KeyDictionary::new();

        state.rebuild_dictionaries();

        assert_eq!(
            Some(addresses[0]),
            state.resolve_node_address(&NodeIndex::from("lorem"))
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_is_referentially_consistent() {
        let (state, _, _) = create_state_with_one_edge();

        assert!(state.is_referentially_consistent());

        let (mut state, node_addresses, edge_address) = create_state_with_one_edge();
        state
            .edge_endpoints
            .get_mut_or_default(edge_address.chunk_index())
            .set(
                edge_address.chunk_local_address(),
                EdgeEndpoints {
                    source_address: NodeAddress::new(999),
                    target_address: node_addresses[1],
                },
            );

        assert!(!state.is_referentially_consistent());

        let (mut state, addresses) = create_state_with_three_nodes();
        state
            .node_memberships
            .get_mut_or_default(addresses[0].chunk_index())
            .add(addresses[0].chunk_local_address(), GroupAddress::new(999));

        assert!(!state.is_referentially_consistent());

        let (mut state, group_address) = create_state_with_one_raw_group();
        state
            .groups
            .record_mut(group_address)
            .unwrap()
            .node_members
            .insert(999);

        assert!(!state.is_referentially_consistent());
    }

    #[test]
    fn test_identity() {
        let first = GraphState::new();
        let second = GraphState::new();

        assert_ne!(first.identity(), second.identity());
    }

    #[test]
    fn test_schema() {
        let state = GraphState::new();

        assert_eq!(&Schema::default(), state.schema().as_ref());
    }

    #[test]
    fn test_node_count() {
        let (state, _) = create_state_with_three_nodes();

        assert_eq!(3, state.node_count());
    }

    #[test]
    fn test_edge_count() {
        let (state, _, _) = create_state_with_one_edge();

        assert_eq!(1, state.edge_count());
    }

    #[test]
    fn test_group_count() {
        let mut state = GraphState::new();

        assert_eq!(0, state.group_count());

        state.groups.add(GroupIndex::from("lorem")).unwrap();

        assert_eq!(1, state.group_count());
    }

    #[test]
    fn test_contains_node_address() {
        let (state, addresses) = create_state_with_three_nodes();

        assert!(state.contains_node_address(addresses[0]));
        assert!(!state.contains_node_address(NodeAddress::new(999)));
    }

    #[test]
    fn test_contains_edge_address() {
        let (state, _, edge_address) = create_state_with_one_edge();

        assert!(state.contains_edge_address(edge_address));
        assert!(!state.contains_edge_address(EdgeAddress::new(999)));
    }

    #[test]
    fn test_resolve_node_address() {
        let (state, _) = create_state_with_three_nodes();

        assert_eq!(
            Some(NodeAddress::new(0)),
            state.resolve_node_address(&NodeIndex::from("lorem"))
        );
        assert_eq!(
            Some(NodeAddress::new(1)),
            state.resolve_node_address(&NodeIndex::from("ipsum"))
        );
        assert_eq!(None, state.resolve_node_address(&NodeIndex::from("sed")));

        let mut state = GraphState::new();
        let lorem_identifier = Identifier::from("lorem");
        let ipsum_identifier = Identifier::from("ipsum");
        let lorem_hash = state
            .node_dictionary
            .hash_one(IdentifierView::from(&lorem_identifier));
        let ipsum_hash = state
            .node_dictionary
            .hash_one(IdentifierView::from(&ipsum_identifier));

        let lorem_address = state.next_node_address;
        state
            .node_keys
            .get_mut_or_default(lorem_address.chunk_index())
            .insert(lorem_address.chunk_local_address(), &lorem_identifier);
        state.next_node_address = NodeAddress::new(lorem_address.index() + 1);
        state.node_count += 1;

        let ipsum_address = state.next_node_address;
        state
            .node_keys
            .get_mut_or_default(ipsum_address.chunk_index())
            .insert(ipsum_address.chunk_local_address(), &ipsum_identifier);
        state.next_node_address = NodeAddress::new(ipsum_address.index() + 1);
        state.node_count += 1;

        state
            .node_dictionary
            .insert(lorem_hash, ipsum_address.index());
        state
            .node_dictionary
            .insert(lorem_hash, lorem_address.index());
        state
            .node_dictionary
            .insert(ipsum_hash, lorem_address.index());
        state
            .node_dictionary
            .insert(ipsum_hash, ipsum_address.index());

        assert_eq!(
            Some(NodeAddress::new(0)),
            state.resolve_node_address(&NodeIndex::from("lorem"))
        );
        assert_eq!(
            Some(NodeAddress::new(1)),
            state.resolve_node_address(&NodeIndex::from("ipsum"))
        );
    }

    #[test]
    fn test_node_key() {
        let (state, addresses) = create_state_with_three_nodes();

        assert_eq!(
            Some(IdentifierView::from(&Identifier::from("lorem"))),
            state.node_key(addresses[0])
        );
        assert_eq!(None, state.node_key(NodeAddress::new(999)));
    }

    #[test]
    fn test_node_attribute() {
        let (mut state, addresses) = create_state_with_three_nodes();
        let attribute_address = state
            .node_attributes
            .resolve_or_insert(&AttributeName::from("sed"));

        assert_eq!(None, state.node_attribute(addresses[0], attribute_address));
    }

    #[test]
    fn test_node_attribute_by_name() {
        let (mut state, addresses) = create_state_with_three_nodes();
        state
            .node_attributes
            .resolve_or_insert(&AttributeName::from("sed"));

        assert_eq!(
            None,
            state.node_attribute_by_name(addresses[0], &AttributeName::from("sed"))
        );
        assert_eq!(
            None,
            state.node_attribute_by_name(addresses[0], &AttributeName::from("missing"))
        );
    }

    #[test]
    fn test_edge_attribute() {
        let (mut state, _, edge_address) = create_state_with_one_edge();
        let attribute_address = state
            .edge_attributes
            .resolve_or_insert(&AttributeName::from("sed"));

        assert_eq!(None, state.edge_attribute(edge_address, attribute_address));
    }

    #[test]
    fn test_edge_attribute_by_name() {
        let (mut state, _, edge_address) = create_state_with_one_edge();
        state
            .edge_attributes
            .resolve_or_insert(&AttributeName::from("sed"));

        assert_eq!(
            None,
            state.edge_attribute_by_name(edge_address, &AttributeName::from("sed"))
        );
        assert_eq!(
            None,
            state.edge_attribute_by_name(edge_address, &AttributeName::from("missing"))
        );
    }

    #[test]
    fn test_node_memberships() {
        let (mut state, group_address) = create_state_with_one_raw_group();
        let node_address = NodeAddress::new(0);

        state
            .node_memberships
            .get_mut_or_default(node_address.chunk_index())
            .add(node_address.chunk_local_address(), group_address);

        assert_eq!(
            vec![GroupAddress::new(0)],
            state.node_memberships(node_address).collect::<Vec<_>>()
        );
        assert_eq!(0, state.node_memberships(NodeAddress::new(1)).count());
    }

    #[test]
    fn test_edge_memberships() {
        let (mut state, group_address) = create_state_with_one_raw_group();
        let edge_address = EdgeAddress::new(0);

        state
            .edge_memberships
            .get_mut_or_default(edge_address.chunk_index())
            .add(edge_address.chunk_local_address(), group_address);

        assert_eq!(
            vec![GroupAddress::new(0)],
            state.edge_memberships(edge_address).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_node_attribute_entries() {
        let mut state = GraphState::new();
        state
            .node_attributes
            .resolve_or_insert(&AttributeName::from("lorem"));

        assert_eq!(
            vec![(AttributeAddress::new(0), AttributeName::from("lorem"))],
            state
                .node_attribute_entries()
                .map(|(address, name)| (address, name.clone()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_edge_attribute_entries() {
        let mut state = GraphState::new();
        state
            .edge_attributes
            .resolve_or_insert(&AttributeName::from("ipsum"));

        assert_eq!(
            vec![(AttributeAddress::new(0), AttributeName::from("ipsum"))],
            state
                .edge_attribute_entries()
                .map(|(address, name)| (address, name.clone()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_resolve_node_attribute_address() {
        let mut state = GraphState::new();
        state
            .node_attributes
            .resolve_or_insert(&AttributeName::from("lorem"));

        assert_eq!(
            Some(AttributeAddress::new(0)),
            state.resolve_node_attribute_address(&AttributeName::from("lorem"))
        );
        assert_eq!(
            None,
            state.resolve_node_attribute_address(&AttributeName::from("missing"))
        );
    }

    #[test]
    fn test_resolve_edge_attribute_address() {
        let mut state = GraphState::new();
        state
            .edge_attributes
            .resolve_or_insert(&AttributeName::from("ipsum"));

        assert_eq!(
            Some(AttributeAddress::new(0)),
            state.resolve_edge_attribute_address(&AttributeName::from("ipsum"))
        );
    }

    #[test]
    fn test_node_attribute_name() {
        let mut state = GraphState::new();
        let attribute_address = state
            .node_attributes
            .resolve_or_insert(&AttributeName::from("lorem"));

        assert_eq!(
            Some(&AttributeName::from("lorem")),
            state.node_attribute_name(attribute_address)
        );
        assert_eq!(None, state.node_attribute_name(AttributeAddress::new(999)));
    }

    #[test]
    fn test_edge_attribute_name() {
        let mut state = GraphState::new();
        let attribute_address = state
            .edge_attributes
            .resolve_or_insert(&AttributeName::from("ipsum"));

        assert_eq!(
            Some(&AttributeName::from("ipsum")),
            state.edge_attribute_name(attribute_address)
        );
        assert_eq!(None, state.edge_attribute_name(AttributeAddress::new(999)));
    }

    #[test]
    fn test_node_addresses() {
        let (state, _) = create_state_with_three_nodes();

        assert_eq!(
            vec![
                NodeAddress::new(0),
                NodeAddress::new(1),
                NodeAddress::new(2)
            ],
            state.node_addresses().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_edge_addresses() {
        let (state, _, _) = create_state_with_one_edge();

        assert_eq!(
            vec![EdgeAddress::new(0)],
            state.edge_addresses().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_edge_endpoints() {
        let (state, _, edge_address) = create_state_with_one_edge();

        assert_eq!(
            Some(EdgeEndpoints {
                source_address: NodeAddress::new(0),
                target_address: NodeAddress::new(1),
            }),
            state.edge_endpoints(edge_address)
        );
        assert_eq!(None, state.edge_endpoints(EdgeAddress::new(999)));
    }

    #[test]
    fn test_outgoing_edge_addresses() {
        let (state, node_addresses, _) = create_state_with_one_edge();

        assert_eq!(
            vec![EdgeAddress::new(0)],
            state
                .outgoing_edge_addresses(node_addresses[0])
                .collect::<Vec<_>>()
        );
        assert_eq!(0, state.outgoing_edge_addresses(node_addresses[2]).count());
    }

    #[test]
    fn test_incoming_edge_addresses() {
        let (state, node_addresses, _) = create_state_with_one_edge();

        assert_eq!(
            vec![EdgeAddress::new(0)],
            state
                .incoming_edge_addresses(node_addresses[1])
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_outgoing_neighbor_addresses() {
        let (state, node_addresses, _) = create_state_with_one_edge();

        assert_eq!(
            vec![NodeAddress::new(1)],
            state
                .outgoing_neighbor_addresses(node_addresses[0])
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_incoming_neighbor_addresses() {
        let (state, node_addresses, _) = create_state_with_one_edge();

        assert_eq!(
            vec![NodeAddress::new(0)],
            state
                .incoming_neighbor_addresses(node_addresses[1])
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_neighbor_addresses() {
        let (state, node_addresses, _) = create_state_with_one_edge();

        assert_eq!(
            vec![NodeAddress::new(1)],
            state
                .neighbor_addresses(node_addresses[0])
                .collect::<Vec<_>>()
        );
        assert_eq!(0, state.neighbor_addresses(node_addresses[2]).count());
    }

    #[test]
    fn test_edge_index() {
        let (state, _, edge_address) = create_state_with_one_edge();

        assert!(state.edge_index(edge_address).is_some());
        assert_eq!(None, state.edge_index(EdgeAddress::new(999)));

        let (mut state, _, edge_address) = create_state_with_one_edge();
        state
            .edge_endpoints
            .get_mut_or_default(edge_address.chunk_index())
            .remove(edge_address.chunk_local_address());

        assert_eq!(None, state.edge_index(edge_address));
    }

    #[test]
    fn test_resolve_edge_address() {
        let (mut state, _, edge_address) = create_state_with_one_edge();
        let edge_index = state.edge_index(edge_address).unwrap();

        assert_eq!(
            Some(EdgeAddress::new(0)),
            state.resolve_edge_address(&edge_index)
        );

        let foreign_index = EdgeIndex::new(edge_index.tag().wrapping_add(1), edge_index.offset());
        assert_eq!(None, state.resolve_edge_address(&foreign_index));

        state
            .edge_endpoints
            .get_mut_or_default(edge_address.chunk_index())
            .remove(edge_address.chunk_local_address());

        assert_eq!(None, state.resolve_edge_address(&edge_index));
    }

    #[test]
    fn test_resolve_group_address() {
        let (state, _) = create_state_with_one_raw_group();

        assert_eq!(
            Some(GroupAddress::new(0)),
            state.resolve_group_address(&GroupIndex::from("lorem"))
        );
        assert_eq!(
            None,
            state.resolve_group_address(&GroupIndex::from("ipsum"))
        );
    }

    #[test]
    fn test_group_index() {
        let (state, group_address) = create_state_with_one_raw_group();

        assert_eq!(
            Some(&GroupIndex::from("lorem")),
            state.group_index(group_address)
        );
        assert_eq!(None, state.group_index(GroupAddress::new(999)));
    }

    #[test]
    fn test_group_addresses() {
        let (state, _) = create_state_with_one_raw_group();

        assert_eq!(
            vec![GroupAddress::new(0)],
            state.group_addresses().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_group_node_member_addresses() {
        let (mut state, group_address) = create_state_with_one_raw_group();
        state
            .groups
            .record_mut(group_address)
            .unwrap()
            .node_members
            .insert(0);

        assert_eq!(
            vec![NodeAddress::new(0)],
            state
                .group_node_member_addresses(group_address)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            0,
            state
                .group_node_member_addresses(GroupAddress::new(999))
                .count()
        );
    }

    #[test]
    fn test_group_edge_member_addresses() {
        let (mut state, group_address) = create_state_with_one_raw_group();
        state
            .groups
            .record_mut(group_address)
            .unwrap()
            .edge_members
            .insert(0);

        assert_eq!(
            vec![EdgeAddress::new(0)],
            state
                .group_edge_member_addresses(group_address)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            0,
            state
                .group_edge_member_addresses(GroupAddress::new(999))
                .count()
        );
    }

    #[test]
    fn test_group_node_member_count() {
        let (mut state, group_address) = create_state_with_one_raw_group();
        state
            .groups
            .record_mut(group_address)
            .unwrap()
            .node_members
            .insert(0);

        assert_eq!(Some(1), state.group_node_member_count(group_address));
        assert_eq!(None, state.group_node_member_count(GroupAddress::new(999)));
    }

    #[test]
    fn test_group_edge_member_count() {
        let (mut state, group_address) = create_state_with_one_raw_group();
        state
            .groups
            .record_mut(group_address)
            .unwrap()
            .edge_members
            .insert(0);

        assert_eq!(Some(1), state.group_edge_member_count(group_address));
        assert_eq!(None, state.group_edge_member_count(GroupAddress::new(999)));
    }

    #[test]
    fn test_default() {
        assert_eq!(0, GraphState::default().node_count());
    }

    #[test]
    fn test_clone() {
        let (state, addresses) = create_state_with_three_nodes();
        let mut cloned = state.clone();

        cloned
            .node_keys
            .get_mut_or_default(addresses[0].chunk_index())
            .remove(addresses[0].chunk_local_address());

        assert!(state.contains_node_address(addresses[0]));
        assert!(!cloned.contains_node_address(addresses[0]));
    }
    #[test]
    fn test_insert_node() {
        let mut state = GraphState::new();

        let address = state
            .insert_node(&NodeIndex::from("lorem"), &create_lorem_attributes(), &[])
            .unwrap();

        assert_eq!(1, state.node_count());
        assert_eq!(
            Some(NodeAddress::new(0)),
            state.resolve_node_address(&NodeIndex::from("lorem"))
        );
        assert_eq!(1, state.groups.ungrouped_node_count());
        assert_eq!(
            Some(Value::Int(42)),
            state
                .node_attribute_by_name(address, &AttributeName::from("lorem"))
                .map(Value::from)
        );
    }

    #[test]
    fn test_invalid_insert_node() {
        let mut state = GraphState::new();
        state
            .insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[])
            .unwrap();

        let result = state.insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeAlreadyExists { node_index }
                if node_index == "lorem".into()
        )));

        let mut state = GraphState::new();
        state.next_node_address = NodeAddress::new(u32::MAX);

        let result = state.insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[]);

        assert!(
            result.is_err_and(|error| matches!(error, GraphRecordError::AddressSpaceExhausted))
        );
    }

    #[test]
    fn test_remove_node() {
        let (mut state, first_address, _) = create_state_with_two_nodes();

        let address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        state.remove_node(address);

        assert_eq!(1, state.node_count());
        assert_eq!(None, state.resolve_node_address(&NodeIndex::from("lorem")));
        assert_eq!(None, state.node_key(first_address));
        assert_eq!(
            None,
            state.node_attribute_by_name(first_address, &AttributeName::from("lorem"))
        );

        let (mut state, first_address, second_address, edge_address) =
            create_state_with_one_inserted_edge();

        let address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        state.remove_node(address);

        assert_eq!(0, state.edge_count());
        assert!(!state.contains_edge_address(edge_address));
        assert_eq!(0, state.incoming_edge_addresses(second_address).count());
        assert!(state.contains_node_address(second_address));
        assert!(!state.contains_node_address(first_address));

        let mut state = GraphState::new();
        let address = state
            .insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[])
            .unwrap();
        state
            .insert_edge(address, address, &AttributeMap::new(), &[])
            .unwrap();

        let address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        state.remove_node(address);

        assert_eq!(0, state.edge_count());
        assert_eq!(0, state.node_count());

        let (mut state, group_address) = create_state_with_one_group();
        let address = state
            .insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[])
            .unwrap();
        state.add_node_to_group(address, group_address).unwrap();

        assert_eq!(0, state.groups.ungrouped_node_count());

        let address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        state.remove_node(address);

        assert_eq!(0, state.groups.ungrouped_node_count());
        assert_eq!(0, state.node_count());

        let (mut state, _, _) = create_state_with_two_nodes();

        assert_eq!(2, state.groups.ungrouped_node_count());

        let address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        state.remove_node(address);

        assert_eq!(1, state.groups.ungrouped_node_count());

        let (mut state, group_address) = create_state_with_one_group();
        state
            .insert_node(
                &NodeIndex::from("amet"),
                &AttributeMap::from([("sed".into(), 7.into())]),
                &[group_address],
            )
            .unwrap();

        assert_eq!(0, state.groups.ungrouped_node_count());

        let address = state
            .resolve_node_address(&NodeIndex::from("amet"))
            .unwrap();
        state.remove_node(address);

        assert_eq!(0, state.groups.ungrouped_node_count());
        assert!(
            state
                .schema
                .group(&GroupIndex::from("dolor"))
                .expect("Group types must be retained.")
                .nodes()
                .contains_key(&AttributeName::from("sed"))
        );
        // A removed node never becomes ungrouped, so its attributes must not appear
        // in the ungrouped schema.
        assert!(
            !state
                .schema
                .ungrouped()
                .nodes()
                .contains_key(&AttributeName::from("sed"))
        );
    }

    #[test]
    #[should_panic(expected = "Node must exist.")]
    fn test_invalid_remove_node() {
        let mut state = GraphState::new();

        state.remove_node(NodeAddress::new(0));
    }

    #[test]
    fn test_insert_edges() {
        let (mut state, _, _) = create_state_with_two_nodes();

        let first_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        let second_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();

        state
            .insert_edges(
                vec![
                    (first_address, second_address, &create_lorem_attributes()),
                    (second_address, first_address, &AttributeMap::new()),
                ],
                &[],
            )
            .unwrap();

        assert_eq!(2, state.edge_count());
        let first_edge_index = state.edge_index(EdgeAddress::new(0)).unwrap();
        let second_edge_index = state.edge_index(EdgeAddress::new(1)).unwrap();
        assert_eq!(
            Some(EdgeAddress::new(0)),
            state.resolve_edge_address(&first_edge_index)
        );
        assert_eq!(
            Some(EdgeAddress::new(1)),
            state.resolve_edge_address(&second_edge_index)
        );
        assert_eq!(
            Some(Value::Int(42)),
            state
                .edge_attribute_by_name(EdgeAddress::new(0), &AttributeName::from("lorem"))
                .map(Value::from)
        );
    }

    #[test]
    fn test_insert_edge() {
        let (state, first_address, second_address, _) = create_state_with_one_inserted_edge();

        assert_eq!(1, state.edge_count());
        assert_eq!(
            vec![EdgeAddress::new(0)],
            state
                .outgoing_edge_addresses(first_address)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![EdgeAddress::new(0)],
            state
                .incoming_edge_addresses(second_address)
                .collect::<Vec<_>>()
        );
        assert_eq!(1, state.groups.ungrouped_edge_count());
    }

    #[test]
    fn test_invalid_insert_edge() {
        let (mut state, first_address, second_address) = create_state_with_two_nodes();
        state.next_edge_address = EdgeAddress::new(u32::MAX);

        let result = state.insert_edge(first_address, second_address, &AttributeMap::new(), &[]);

        assert!(
            result.is_err_and(|error| matches!(error, GraphRecordError::AddressSpaceExhausted))
        );
    }

    #[test]
    fn test_remove_edge() {
        let (mut state, first_address, second_address, edge_address) =
            create_state_with_one_inserted_edge();

        state.remove_edge(edge_address);

        assert_eq!(0, state.edge_count());
        assert!(!state.contains_edge_address(edge_address));
        assert_eq!(0, state.outgoing_edge_addresses(first_address).count());
        assert_eq!(0, state.incoming_edge_addresses(second_address).count());
        assert_eq!(0, state.groups.ungrouped_edge_count());

        let (mut state, group_address, _, _, edge_address) =
            create_state_with_one_group_and_one_edge();
        state
            .add_edge_to_group(edge_address, group_address)
            .unwrap();

        assert_eq!(0, state.groups.ungrouped_edge_count());

        state.remove_edge(edge_address);

        assert_eq!(0, state.groups.ungrouped_edge_count());
    }

    #[test]
    #[should_panic(expected = "Edge must exist.")]
    fn test_invalid_remove_edge() {
        let mut state = GraphState::new();

        state.remove_edge(EdgeAddress::new(0));
    }

    #[test]
    fn test_add_node_to_group() {
        let (mut state, group_address) = create_state_with_one_group();
        let address = state
            .insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[])
            .unwrap();

        state.add_node_to_group(address, group_address).unwrap();

        assert_eq!(
            vec![GroupAddress::new(0)],
            state.node_memberships(address).collect::<Vec<_>>()
        );
        assert!(
            state
                .groups
                .record(group_address)
                .unwrap()
                .node_members
                .contains(address.index())
        );
        assert_eq!(0, state.groups.ungrouped_node_count());
    }

    #[test]
    fn test_invalid_add_node_to_group() {
        let (mut state, group_address) = create_state_with_one_group();
        let address = state
            .insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[])
            .unwrap();
        state.add_node_to_group(address, group_address).unwrap();

        let result = state.add_node_to_group(address, group_address);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeAlreadyInGroup { node_index, group_index }
                if node_index == "lorem".into() && group_index == "dolor".into()
        )));
    }

    #[test]
    fn test_remove_node_from_group() {
        let (mut state, group_address) = create_state_with_one_group();
        let address = state
            .insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[])
            .unwrap();
        state.add_node_to_group(address, group_address).unwrap();

        state
            .remove_node_from_group(address, group_address)
            .unwrap();

        assert_eq!(0, state.node_memberships(address).count());
        assert!(
            !state
                .groups
                .record(group_address)
                .unwrap()
                .node_members
                .contains(address.index())
        );
        assert_eq!(1, state.groups.ungrouped_node_count());
    }

    #[test]
    fn test_invalid_remove_node_from_group() {
        let (mut state, group_address) = create_state_with_one_group();
        let address = state
            .insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[])
            .unwrap();

        let result = state.remove_node_from_group(address, group_address);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeNotInGroup { node_index, group_index }
                if node_index == "lorem".into() && group_index == "dolor".into()
        )));
    }

    #[test]
    fn test_add_edge_to_group() {
        let (mut state, group_address, _, _, edge_address) =
            create_state_with_one_group_and_one_edge();

        state
            .add_edge_to_group(edge_address, group_address)
            .unwrap();

        assert_eq!(
            vec![GroupAddress::new(0)],
            state.edge_memberships(edge_address).collect::<Vec<_>>()
        );
        assert!(
            state
                .groups
                .record(group_address)
                .unwrap()
                .edge_members
                .contains(edge_address.index())
        );
        assert_eq!(0, state.groups.ungrouped_edge_count());
    }

    #[test]
    fn test_invalid_add_edge_to_group() {
        let (mut state, group_address, _, _, edge_address) =
            create_state_with_one_group_and_one_edge();
        state
            .add_edge_to_group(edge_address, group_address)
            .unwrap();

        let edge_index = state
            .edge_index(edge_address)
            .expect("Edge must belong to an epoch.");
        let result = state.add_edge_to_group(edge_address, group_address);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::EdgeAlreadyInGroup {
                edge_index: found_edge_index,
                group_index
            } if found_edge_index == edge_index && group_index == "dolor".into()
        )));
    }

    #[test]
    fn test_remove_edge_from_group() {
        let (mut state, group_address, _, _, edge_address) =
            create_state_with_one_group_and_one_edge();
        state
            .add_edge_to_group(edge_address, group_address)
            .unwrap();

        state
            .remove_edge_from_group(edge_address, group_address)
            .unwrap();

        assert_eq!(0, state.edge_memberships(edge_address).count());
        assert!(
            !state
                .groups
                .record(group_address)
                .unwrap()
                .edge_members
                .contains(edge_address.index())
        );
        assert_eq!(1, state.groups.ungrouped_edge_count());
    }

    #[test]
    fn test_invalid_remove_edge_from_group() {
        let (mut state, group_address, _, _, edge_address) =
            create_state_with_one_group_and_one_edge();

        let edge_index = state
            .edge_index(edge_address)
            .expect("Edge must belong to an epoch.");
        let result = state.remove_edge_from_group(edge_address, group_address);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::EdgeNotInGroup {
                edge_index: found_edge_index,
                group_index
            } if found_edge_index == edge_index && group_index == "dolor".into()
        )));
    }

    #[test]
    fn test_insert_group() {
        let mut state = GraphState::new();

        state.insert_group(&GroupIndex::from("lorem")).unwrap();

        assert_eq!(1, state.group_count());
        assert_eq!(
            Some(GroupAddress::new(0)),
            state.resolve_group_address(&GroupIndex::from("lorem"))
        );
        assert!(
            state
                .schema
                .groups()
                .contains_key(&GroupIndex::from("lorem"))
        );

        let (mut state, group_address) = create_state_with_one_group();
        state
            .insert_node(
                &NodeIndex::from("amet"),
                &AttributeMap::from([("sed".into(), 7.into())]),
                &[group_address],
            )
            .unwrap();

        state.remove_group(group_address).unwrap();
        state.insert_group(&GroupIndex::from("dolor")).unwrap();

        // The schema keeps a removed group's accumulated types; re-adding a group
        // under the same name reuses them.
        assert!(
            state
                .schema
                .group(&GroupIndex::from("dolor"))
                .expect("Group types must be reused.")
                .nodes()
                .contains_key(&AttributeName::from("sed"))
        );
    }

    #[test]
    fn test_invalid_insert_group() {
        let mut state = GraphState::new();
        state.insert_group(&GroupIndex::from("lorem")).unwrap();

        let result = state.insert_group(&GroupIndex::from("lorem"));

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::GroupAlreadyExists { group_index }
                if group_index == "lorem".into()
        )));

        let mut state = GraphState::new();
        state.schema = Arc::new(Schema::new_provided(HashMap::new(), GroupSchema::default()));

        let result = state.insert_group(&GroupIndex::from("lorem"));

        assert!(result.is_err());
        assert_eq!(1, state.group_count());
    }

    #[test]
    fn test_remove_group() {
        let (mut state, group_address) = create_state_with_one_group();
        let node_address = state
            .insert_node(&NodeIndex::from("lorem"), &create_lorem_attributes(), &[])
            .unwrap();
        state
            .add_node_to_group(node_address, group_address)
            .unwrap();

        assert_eq!(0, state.groups.ungrouped_node_count());

        state.remove_group(group_address).unwrap();

        assert_eq!(0, state.group_count());
        assert_eq!(0, state.node_memberships(node_address).count());
        assert_eq!(1, state.groups.ungrouped_node_count());
        assert!(state.contains_node_address(node_address));
        assert!(
            state
                .schema
                .ungrouped()
                .nodes()
                .contains_key(&AttributeName::from("lorem"))
        );
    }

    #[test]
    fn test_set_node_attributes() {
        let (mut state, first_address, _) = create_state_with_two_nodes();

        state
            .set_node_attributes(
                first_address,
                &AttributeMap::from([("ipsum".into(), true.into())]),
            )
            .unwrap();

        assert!(
            state
                .schema
                .ungrouped()
                .nodes()
                .contains_key(&AttributeName::from("ipsum"))
        );
        assert_eq!(
            Some(Value::Bool(true)),
            state
                .node_attribute_by_name(first_address, &AttributeName::from("ipsum"))
                .map(Value::from)
        );

        let (mut state, group_address) = create_state_with_one_group();
        let address = state
            .insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[])
            .unwrap();
        state.add_node_to_group(address, group_address).unwrap();

        state
            .set_node_attributes(address, &create_lorem_attributes())
            .unwrap();

        assert!(
            state
                .schema
                .group(&GroupIndex::from("dolor"))
                .unwrap()
                .nodes()
                .contains_key(&AttributeName::from("lorem"))
        );
    }

    #[test]
    fn test_invalid_set_node_attributes() {
        let (mut state, first_address, _) = create_state_with_two_nodes();
        state.schema = Arc::new(Schema::new_provided(
            HashMap::new(),
            create_provided_group_schema(),
        ));

        let result = state.set_node_attributes(
            first_address,
            &AttributeMap::from([("ipsum".into(), true.into())]),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_replace_node_attributes() {
        let (mut state, first_address, _) = create_state_with_two_nodes();

        state
            .replace_node_attributes(first_address, &AttributeMap::new())
            .unwrap();

        assert_eq!(
            None,
            state.node_attribute_by_name(first_address, &AttributeName::from("lorem"))
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::Int)),
            state
                .schema
                .ungrouped()
                .nodes()
                .get(&AttributeName::from("lorem"))
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_remove_node_attribute() {
        let (mut state, first_address, _) = create_state_with_two_nodes();

        state
            .remove_node_attribute(first_address, &AttributeName::from("lorem"))
            .unwrap();

        assert_eq!(
            None,
            state.node_attribute_by_name(first_address, &AttributeName::from("lorem"))
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::Int)),
            state
                .schema
                .ungrouped()
                .nodes()
                .get(&AttributeName::from("lorem"))
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_invalid_remove_node_attribute() {
        let (mut state, first_address, _) = create_state_with_two_nodes();

        let result = state.remove_node_attribute(first_address, &AttributeName::from("missing"));

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeAttributeNotFound {
                node_index,
                attribute_name
            } if node_index == "lorem".into() && attribute_name == "missing".into()
        )));
    }

    #[test]
    fn test_set_edge_attributes() {
        let (mut state, _, _, edge_address) = create_state_with_one_inserted_edge();

        state
            .set_edge_attributes(
                edge_address,
                &AttributeMap::from([("ipsum".into(), true.into())]),
            )
            .unwrap();

        assert!(
            state
                .schema
                .ungrouped()
                .edges()
                .contains_key(&AttributeName::from("ipsum"))
        );

        let (mut state, group_address) = create_state_with_one_group();
        let first_address = state
            .insert_node(&NodeIndex::from("lorem"), &AttributeMap::new(), &[])
            .unwrap();
        let second_address = state
            .insert_node(&NodeIndex::from("ipsum"), &AttributeMap::new(), &[])
            .unwrap();
        let edge_address = state
            .insert_edge(first_address, second_address, &AttributeMap::new(), &[])
            .unwrap();
        state
            .add_edge_to_group(edge_address, group_address)
            .unwrap();

        state
            .set_edge_attributes(edge_address, &create_lorem_attributes())
            .unwrap();

        assert!(
            state
                .schema
                .group(&GroupIndex::from("dolor"))
                .unwrap()
                .edges()
                .contains_key(&AttributeName::from("lorem"))
        );
    }

    #[test]
    fn test_replace_edge_attributes() {
        let (mut state, _, _, edge_address) = create_state_with_one_inserted_edge();

        state
            .replace_edge_attributes(edge_address, &AttributeMap::new())
            .unwrap();

        assert_eq!(
            None,
            state.edge_attribute_by_name(edge_address, &AttributeName::from("lorem"))
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::Int)),
            state
                .schema
                .ungrouped()
                .edges()
                .get(&AttributeName::from("lorem"))
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_remove_edge_attribute() {
        let (mut state, _, _, edge_address) = create_state_with_one_inserted_edge();

        state
            .remove_edge_attribute(edge_address, &AttributeName::from("lorem"))
            .unwrap();

        assert_eq!(
            None,
            state.edge_attribute_by_name(edge_address, &AttributeName::from("lorem"))
        );
        assert_eq!(
            &DataType::Option(Box::new(DataType::Int)),
            state
                .schema
                .ungrouped()
                .edges()
                .get(&AttributeName::from("lorem"))
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_invalid_remove_edge_attribute() {
        let (mut state, _, _, edge_address) = create_state_with_one_inserted_edge();

        let edge_index = state
            .edge_index(edge_address)
            .expect("Edge must belong to an epoch.");
        let result = state.remove_edge_attribute(edge_address, &AttributeName::from("missing"));

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::EdgeAttributeNotFound {
                edge_index: found_edge_index,
                attribute_name
            } if found_edge_index == edge_index && attribute_name == "missing".into()
        )));
    }

    #[test]
    fn test_node_attribute_map() {
        let (state, first_address, _) = create_state_with_two_nodes();

        assert_eq!(
            AttributeMap::from([("lorem".into(), 42.into())]),
            state.node_attribute_map(first_address)
        );
    }

    #[test]
    fn test_edge_attribute_map() {
        let (state, _, _, edge_address) = create_state_with_one_inserted_edge();

        assert_eq!(
            AttributeMap::from([("lorem".into(), 42.into())]),
            state.edge_attribute_map(edge_address)
        );
    }

    #[test]
    fn test_freeze_schema() {
        let mut state = GraphState::new();

        state.freeze_schema();

        assert_eq!(SchemaType::Provided, *state.schema.schema_type());
    }

    #[test]
    fn test_unfreeze_schema() {
        let mut state = GraphState::new();
        state.freeze_schema();

        state.unfreeze_schema();

        assert_eq!(SchemaType::Inferred, *state.schema.schema_type());
    }

    #[test]
    fn test_replace_schema() {
        let mut state = GraphState::new();

        state.replace_schema(Arc::new(Schema::new_provided(
            HashMap::new(),
            create_provided_group_schema(),
        )));

        assert_eq!(
            Schema::new_provided(HashMap::new(), create_provided_group_schema()),
            *state.schema
        );
    }

    #[test]
    fn test_clear_content() {
        let (mut state, _, _, _) = create_state_with_one_inserted_edge();
        let schema_before = Arc::clone(&state.schema);

        state.clear_content();

        assert_eq!(0, state.node_count());
        assert_eq!(0, state.edge_count());
        assert_eq!(0, state.group_count());
        assert_eq!(0, state.node_addresses().count());
        assert!(Arc::ptr_eq(&schema_before, &state.schema));
    }

    #[test]
    fn test_compact() {
        let (mut state, group_address) = create_state_with_one_group();

        let first_address = state
            .insert_node(&NodeIndex::from("lorem"), &create_lorem_attributes(), &[])
            .unwrap();
        let second_address = state
            .insert_node(&NodeIndex::from("ipsum"), &AttributeMap::new(), &[])
            .unwrap();
        let third_address = state
            .insert_node(&NodeIndex::from("dolor"), &AttributeMap::new(), &[])
            .unwrap();

        let first_edge_address = state
            .insert_edge(
                first_address,
                second_address,
                &create_lorem_attributes(),
                &[],
            )
            .unwrap();
        state
            .insert_edge(second_address, third_address, &AttributeMap::new(), &[])
            .unwrap();
        state.append_edge_epoch(EdgeAddress::new(0), 2);

        state
            .add_node_to_group(first_address, group_address)
            .unwrap();
        state
            .add_edge_to_group(first_edge_address, group_address)
            .unwrap();

        let first_edge_index = state.edge_index(first_edge_address).unwrap();

        let address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();
        state.remove_node(address);

        state.compact();

        assert_eq!(2, state.node_count());
        assert_eq!(0, state.edge_count());
        assert_eq!(1, state.group_count());
        assert_eq!(NodeAddress::new(2), state.next_node_address);
        assert_eq!(EdgeAddress::new(0), state.next_edge_address);

        let new_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        assert_eq!(NodeAddress::new(0), new_address);
        assert_eq!(
            Some(Value::Int(42)),
            state
                .node_attribute_by_name(new_address, &AttributeName::from("lorem"))
                .map(Value::from)
        );

        let new_group_address = state
            .resolve_group_address(&GroupIndex::from("dolor"))
            .unwrap();
        assert_eq!(GroupAddress::new(0), new_group_address);
        assert_eq!(
            vec![GroupAddress::new(0)],
            state.node_memberships(new_address).collect::<Vec<_>>()
        );

        assert_eq!(None, state.resolve_edge_address(&first_edge_index));

        let (mut state, _, second_address, _) = create_state_with_one_inserted_edge();
        let third_address = state
            .insert_node(&NodeIndex::from("dolor"), &AttributeMap::new(), &[])
            .unwrap();
        let second_edge_address = state
            .insert_edge(second_address, third_address, &AttributeMap::new(), &[])
            .unwrap();
        state.append_edge_epoch(EdgeAddress::new(0), 2);
        let second_edge_index_before = state.edge_index(second_edge_address).unwrap();

        let address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        state.remove_node(address);

        state.compact();

        assert_eq!(1, state.edge_count());
        assert_eq!(None, state.resolve_edge_address(&second_edge_index_before));

        let remaining_node_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();
        let new_edge_address = state
            .outgoing_edge_addresses(remaining_node_address)
            .next()
            .unwrap();
        let new_edge_index = state.edge_index(new_edge_address).unwrap();

        assert_eq!(EdgeAddress::new(0), new_edge_address);
        assert_eq!(
            Some(EdgeAddress::new(0)),
            state.resolve_edge_address(&new_edge_index)
        );
    }

    #[test]
    fn test_append_edge_epoch() {
        let mut state = GraphState::new();

        state.append_edge_epoch(EdgeAddress::new(0), 0);

        assert_eq!(0, state.edge_epochs.len());

        let (mut state, first_address, second_address) = create_state_with_two_nodes();
        let edge_address = state
            .insert_edge(first_address, second_address, &AttributeMap::new(), &[])
            .unwrap();

        state.append_edge_epoch(edge_address, 1);

        let edge_index = state.edge_index(edge_address).unwrap();
        assert_eq!(
            Some(EdgeAddress::new(0)),
            state.resolve_edge_address(&edge_index)
        );
    }
}
