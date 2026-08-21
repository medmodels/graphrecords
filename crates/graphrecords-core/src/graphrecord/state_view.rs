use crate::graphrecord::{
    GraphRecord,
    datatypes::{
        AttributeName, AttributeNameView, EdgeIndex, GroupIndex, GroupIndexView, NodeIndexView,
        ValueView,
    },
    state::{AttributeAddress, EdgeAddress, GraphState, GroupAddress, NodeAddress, StateIdentity},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeAttributeAddress(AttributeAddress);

impl NodeAttributeAddress {
    #[must_use]
    pub const fn index(&self) -> u32 {
        self.0.index()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeAttributeAddress(AttributeAddress);

impl EdgeAttributeAddress {
    #[must_use]
    pub const fn index(&self) -> u32 {
        self.0.index()
    }
}

#[derive(Clone, Copy)]
pub struct StateView<'a> {
    graphrecord: &'a GraphRecord,
}

impl<'a> StateView<'a> {
    #[must_use]
    pub const fn of(graphrecord: &'a GraphRecord) -> Self {
        Self { graphrecord }
    }

    #[must_use]
    pub fn state_identity(&self) -> StateIdentity {
        self.state().identity()
    }

    pub fn node_addresses(&self) -> impl Iterator<Item = NodeAddress> + use<'a> {
        self.state().node_addresses()
    }

    pub fn edge_addresses(&self) -> impl Iterator<Item = EdgeAddress> + use<'a> {
        self.state().edge_addresses()
    }

    pub fn group_addresses(&self) -> impl Iterator<Item = GroupAddress> + use<'a> {
        self.state().group_addresses()
    }

    pub fn node_attribute_addresses(&self) -> impl Iterator<Item = NodeAttributeAddress> + use<'a> {
        self.state()
            .node_attribute_entries()
            .map(|(address, _)| NodeAttributeAddress(address))
    }

    pub fn edge_attribute_addresses(&self) -> impl Iterator<Item = EdgeAttributeAddress> + use<'a> {
        self.state()
            .edge_attribute_entries()
            .map(|(address, _)| EdgeAttributeAddress(address))
    }

    #[must_use]
    pub fn resolve_node_address<'b>(
        &self,
        node_index: impl Into<NodeIndexView<'b>>,
    ) -> Option<NodeAddress> {
        self.state().resolve_node_address(node_index)
    }

    #[must_use]
    pub fn resolve_edge_address(&self, edge_index: &EdgeIndex) -> Option<EdgeAddress> {
        self.state().resolve_edge_address(edge_index)
    }

    #[must_use]
    pub fn resolve_group_address<'b>(
        &self,
        group_index: impl Into<GroupIndexView<'b>>,
    ) -> Option<GroupAddress> {
        self.state().resolve_group_address(group_index)
    }

    #[must_use]
    pub fn resolve_node_attribute_address<'b>(
        &self,
        attribute_name: impl Into<AttributeNameView<'b>>,
    ) -> Option<NodeAttributeAddress> {
        self.state()
            .resolve_node_attribute_address(attribute_name)
            .map(NodeAttributeAddress)
    }

    #[must_use]
    pub fn resolve_edge_attribute_address<'b>(
        &self,
        attribute_name: impl Into<AttributeNameView<'b>>,
    ) -> Option<EdgeAttributeAddress> {
        self.state()
            .resolve_edge_attribute_address(attribute_name)
            .map(EdgeAttributeAddress)
    }

    /// # Panics
    ///
    /// Panics if `node_address` does not name a live node in this record's state.
    #[must_use]
    pub fn node_index(&self, node_address: NodeAddress) -> NodeIndexView<'a> {
        self.state()
            .node_key(node_address)
            .map(NodeIndexView::from)
            .expect("Node must exist.")
    }

    /// # Panics
    ///
    /// Panics if `edge_address` does not name a live edge in this record's state.
    #[must_use]
    pub fn edge_index(&self, edge_address: EdgeAddress) -> EdgeIndex {
        self.state()
            .edge_index(edge_address)
            .expect("Edge must exist.")
    }

    /// # Panics
    ///
    /// Panics if `group_address` does not name a live group in this record's state.
    #[must_use]
    pub fn group_index(&self, group_address: GroupAddress) -> &'a GroupIndex {
        self.state()
            .group_index(group_address)
            .expect("Group must exist.")
    }

    /// # Panics
    ///
    /// Panics if `attribute_address` does not name a live node attribute in this record's state.
    #[must_use]
    pub fn node_attribute_name(
        &self,
        attribute_address: NodeAttributeAddress,
    ) -> &'a AttributeName {
        self.state()
            .node_attribute_name(attribute_address.0)
            .expect("Attribute must exist.")
    }

    /// # Panics
    ///
    /// Panics if `attribute_address` does not name a live edge attribute in this record's state.
    #[must_use]
    pub fn edge_attribute_name(
        &self,
        attribute_address: EdgeAttributeAddress,
    ) -> &'a AttributeName {
        self.state()
            .edge_attribute_name(attribute_address.0)
            .expect("Attribute must exist.")
    }

    #[must_use]
    pub fn node_attribute(
        &self,
        node_address: NodeAddress,
        attribute_address: NodeAttributeAddress,
    ) -> Option<ValueView<'a>> {
        self.state()
            .node_attribute(node_address, attribute_address.0)
    }

    #[must_use]
    pub fn edge_attribute(
        &self,
        edge_address: EdgeAddress,
        attribute_address: EdgeAttributeAddress,
    ) -> Option<ValueView<'a>> {
        self.state()
            .edge_attribute(edge_address, attribute_address.0)
    }

    pub fn node_group_addresses(
        &self,
        node_address: NodeAddress,
    ) -> impl Iterator<Item = GroupAddress> + use<'a> {
        self.state().node_memberships(node_address)
    }

    pub fn edge_group_addresses(
        &self,
        edge_address: EdgeAddress,
    ) -> impl Iterator<Item = GroupAddress> + use<'a> {
        self.state().edge_memberships(edge_address)
    }

    pub fn group_node_addresses(
        &self,
        group_address: GroupAddress,
    ) -> impl Iterator<Item = NodeAddress> + use<'a> {
        self.state().group_node_member_addresses(group_address)
    }

    pub fn group_edge_addresses(
        &self,
        group_address: GroupAddress,
    ) -> impl Iterator<Item = EdgeAddress> + use<'a> {
        self.state().group_edge_member_addresses(group_address)
    }

    /// # Panics
    ///
    /// Panics if `group_address` does not name a live group in this record's state.
    #[must_use]
    pub fn group_node_count(&self, group_address: GroupAddress) -> usize {
        self.state()
            .group_node_member_count(group_address)
            .expect("Group must exist.")
    }

    /// # Panics
    ///
    /// Panics if `group_address` does not name a live group in this record's state.
    #[must_use]
    pub fn group_edge_count(&self, group_address: GroupAddress) -> usize {
        self.state()
            .group_edge_member_count(group_address)
            .expect("Group must exist.")
    }

    /// # Panics
    ///
    /// Panics if `edge_address` does not name a live edge in this record's state.
    #[must_use]
    pub fn edge_endpoints(&self, edge_address: EdgeAddress) -> (NodeAddress, NodeAddress) {
        let endpoints = self
            .state()
            .edge_endpoints(edge_address)
            .expect("Edge must exist.");

        (endpoints.source_address, endpoints.target_address)
    }

    pub fn outgoing_edge_addresses(
        &self,
        node_address: NodeAddress,
    ) -> impl Iterator<Item = EdgeAddress> + use<'a> {
        self.state().outgoing_edge_addresses(node_address)
    }

    pub fn incoming_edge_addresses(
        &self,
        node_address: NodeAddress,
    ) -> impl Iterator<Item = EdgeAddress> + use<'a> {
        self.state().incoming_edge_addresses(node_address)
    }

    pub fn incident_edge_addresses(
        &self,
        node_address: NodeAddress,
    ) -> impl Iterator<Item = EdgeAddress> + use<'a> {
        self.state()
            .outgoing_edge_addresses(node_address)
            .chain(self.state().incoming_edge_addresses(node_address))
    }

    pub fn outgoing_neighbor_addresses(
        &self,
        node_address: NodeAddress,
    ) -> impl Iterator<Item = NodeAddress> + use<'a> {
        self.state().outgoing_neighbor_addresses(node_address)
    }

    pub fn incoming_neighbor_addresses(
        &self,
        node_address: NodeAddress,
    ) -> impl Iterator<Item = NodeAddress> + use<'a> {
        self.state().incoming_neighbor_addresses(node_address)
    }

    pub fn neighbor_addresses(
        &self,
        node_address: NodeAddress,
    ) -> impl Iterator<Item = NodeAddress> + use<'a> {
        self.state().neighbor_addresses(node_address)
    }

    fn state(self) -> &'a GraphState {
        self.graphrecord.state()
    }
}

#[cfg(test)]
mod test {
    use super::{
        AttributeAddress, EdgeAddress, EdgeAttributeAddress, GroupAddress, NodeAddress,
        NodeAttributeAddress, StateView,
    };
    use crate::graphrecord::{
        AttributeMap, AttributeName, EdgeIndex, GraphRecord, GroupIndex, Identifier, NodeIndex,
        datatypes::{NodeIndexView, ValueView},
    };

    fn create_graphrecord() -> GraphRecord {
        GraphRecord::new()
            .add_group("dolor")
            .unwrap()
            .add_nodes_in_group(
                vec![
                    ("lorem", AttributeMap::from([("sed".into(), 42.into())])),
                    ("ipsum", AttributeMap::new()),
                ],
                "dolor",
            )
            .unwrap()
            .add_edges_in_group(
                vec![(
                    "lorem",
                    "ipsum",
                    AttributeMap::from([("sed".into(), true.into())]),
                )],
                "dolor",
            )
            .unwrap()
    }

    #[test]
    fn test_node_attribute_address_index() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let attribute_address = state
            .resolve_node_attribute_address(&AttributeName::from("sed"))
            .unwrap();

        assert_eq!(0, attribute_address.index());
    }

    #[test]
    fn test_edge_attribute_address_index() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let attribute_address = state
            .resolve_edge_attribute_address(&AttributeName::from("sed"))
            .unwrap();

        assert_eq!(0, attribute_address.index());
    }

    #[test]
    fn test_state_identity() {
        let graphrecord = create_graphrecord();
        let cloned = graphrecord.clone();
        let derived = graphrecord.add_node("sed", AttributeMap::new()).unwrap();

        assert_eq!(
            StateView::of(&graphrecord).state_identity(),
            StateView::of(&cloned).state_identity()
        );
        assert_ne!(
            StateView::of(&graphrecord).state_identity(),
            StateView::of(&derived).state_identity()
        );
    }

    #[test]
    fn test_node_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert_eq!(2, state.node_addresses().count());
    }

    #[test]
    fn test_edge_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert_eq!(1, state.edge_addresses().count());
    }

    #[test]
    fn test_group_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert_eq!(1, state.group_addresses().count());
    }

    #[test]
    fn test_node_attribute_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert_eq!(1, state.node_attribute_addresses().count());
    }

    #[test]
    fn test_edge_attribute_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert_eq!(1, state.edge_attribute_addresses().count());
    }

    #[test]
    fn test_resolve_node_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert!(
            state
                .resolve_node_address(&NodeIndex::from("lorem"))
                .is_some()
        );
    }

    #[test]
    fn test_invalid_resolve_node_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert_eq!(
            None,
            state.resolve_node_address(&NodeIndex::from("missing"))
        );
    }

    #[test]
    fn test_resolve_edge_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let edge_index = graphrecord.edge_indices().next().unwrap();

        assert!(state.resolve_edge_address(&edge_index).is_some());
    }

    #[test]
    fn test_invalid_resolve_edge_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let edge_index = graphrecord.edge_indices().next().unwrap();

        assert_eq!(
            None,
            state.resolve_edge_address(&EdgeIndex::new(
                edge_index.tag().wrapping_add(1),
                edge_index.offset()
            ))
        );
    }

    #[test]
    fn test_resolve_group_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert!(
            state
                .resolve_group_address(&GroupIndex::from("dolor"))
                .is_some()
        );
    }

    #[test]
    fn test_invalid_resolve_group_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert_eq!(
            None,
            state.resolve_group_address(&GroupIndex::from("missing"))
        );
    }

    #[test]
    fn test_resolve_node_attribute_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert!(
            state
                .resolve_node_attribute_address(&AttributeName::from("sed"))
                .is_some()
        );
    }

    #[test]
    fn test_invalid_resolve_node_attribute_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert_eq!(
            None,
            state.resolve_node_attribute_address(&AttributeName::from("missing"))
        );
    }

    #[test]
    fn test_resolve_edge_attribute_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert!(
            state
                .resolve_edge_attribute_address(&AttributeName::from("sed"))
                .is_some()
        );
    }

    #[test]
    fn test_invalid_resolve_edge_attribute_address() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        assert_eq!(
            None,
            state.resolve_edge_attribute_address(&AttributeName::from("missing"))
        );
    }

    #[test]
    fn test_node_index() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();

        assert_eq!(
            NodeIndexView::from(&Identifier::from("lorem")),
            state.node_index(lorem_address)
        );
    }

    #[test]
    #[should_panic(expected = "Node must exist.")]
    fn test_invalid_node_index() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        let _ = state.node_index(NodeAddress::new(999));
    }

    #[test]
    fn test_edge_index() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let edge_index = graphrecord.edge_indices().next().unwrap();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();

        assert_eq!(edge_index, state.edge_index(edge_address));
    }

    #[test]
    #[should_panic(expected = "Edge must exist.")]
    fn test_invalid_edge_index() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        let _ = state.edge_index(EdgeAddress::new(999));
    }

    #[test]
    fn test_group_index() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let group_address = state
            .resolve_group_address(&GroupIndex::from("dolor"))
            .unwrap();

        assert_eq!(&GroupIndex::from("dolor"), state.group_index(group_address));
    }

    #[test]
    #[should_panic(expected = "Group must exist.")]
    fn test_invalid_group_index() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        let _ = state.group_index(GroupAddress::new(999));
    }

    #[test]
    fn test_node_attribute_name() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let attribute_address = state
            .resolve_node_attribute_address(&AttributeName::from("sed"))
            .unwrap();

        assert_eq!(
            &AttributeName::from("sed"),
            state.node_attribute_name(attribute_address)
        );
    }

    #[test]
    #[should_panic(expected = "Attribute must exist.")]
    fn test_invalid_node_attribute_name() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        let _ = state.node_attribute_name(NodeAttributeAddress(AttributeAddress::new(999)));
    }

    #[test]
    fn test_edge_attribute_name() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let attribute_address = state
            .resolve_edge_attribute_address(&AttributeName::from("sed"))
            .unwrap();

        assert_eq!(
            &AttributeName::from("sed"),
            state.edge_attribute_name(attribute_address)
        );
    }

    #[test]
    #[should_panic(expected = "Attribute must exist.")]
    fn test_invalid_edge_attribute_name() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        let _ = state.edge_attribute_name(EdgeAttributeAddress(AttributeAddress::new(999)));
    }

    #[test]
    fn test_node_attribute() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        let ipsum_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();
        let attribute_address = state
            .resolve_node_attribute_address(&AttributeName::from("sed"))
            .unwrap();

        assert_eq!(
            Some(ValueView::Int(42)),
            state.node_attribute(lorem_address, attribute_address)
        );
        assert_eq!(None, state.node_attribute(ipsum_address, attribute_address));
    }

    #[test]
    fn test_edge_attribute() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let edge_index = graphrecord.edge_indices().next().unwrap();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let attribute_address = state
            .resolve_edge_attribute_address(&AttributeName::from("sed"))
            .unwrap();

        assert_eq!(
            Some(ValueView::Bool(true)),
            state.edge_attribute(edge_address, attribute_address)
        );
    }

    #[test]
    fn test_node_group_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let group_address = state
            .resolve_group_address(&GroupIndex::from("dolor"))
            .unwrap();
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();

        assert_eq!(
            vec![group_address],
            state
                .node_group_addresses(lorem_address)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_edge_group_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let group_address = state
            .resolve_group_address(&GroupIndex::from("dolor"))
            .unwrap();
        let edge_index = graphrecord.edge_indices().next().unwrap();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();

        assert_eq!(
            vec![group_address],
            state.edge_group_addresses(edge_address).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_group_node_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let group_address = state
            .resolve_group_address(&GroupIndex::from("dolor"))
            .unwrap();
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        let ipsum_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();

        let mut node_members = state
            .group_node_addresses(group_address)
            .collect::<Vec<_>>();
        node_members.sort_by_key(NodeAddress::index);

        let mut expected_node_members = vec![lorem_address, ipsum_address];
        expected_node_members.sort_by_key(NodeAddress::index);

        assert_eq!(expected_node_members, node_members);
    }

    #[test]
    fn test_group_edge_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let group_address = state
            .resolve_group_address(&GroupIndex::from("dolor"))
            .unwrap();
        let edge_index = graphrecord.edge_indices().next().unwrap();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();

        assert_eq!(
            vec![edge_address],
            state
                .group_edge_addresses(group_address)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_group_node_count() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let group_address = state
            .resolve_group_address(&GroupIndex::from("dolor"))
            .unwrap();

        assert_eq!(2, state.group_node_count(group_address));
    }

    #[test]
    #[should_panic(expected = "Group must exist.")]
    fn test_invalid_group_node_count() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        let _ = state.group_node_count(GroupAddress::new(999));
    }

    #[test]
    fn test_group_edge_count() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let group_address = state
            .resolve_group_address(&GroupIndex::from("dolor"))
            .unwrap();

        assert_eq!(1, state.group_edge_count(group_address));
    }

    #[test]
    #[should_panic(expected = "Group must exist.")]
    fn test_invalid_group_edge_count() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        let _ = state.group_edge_count(GroupAddress::new(999));
    }

    #[test]
    fn test_edge_endpoints() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let edge_index = graphrecord.edge_indices().next().unwrap();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        let ipsum_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();

        assert_eq!(
            (lorem_address, ipsum_address),
            state.edge_endpoints(edge_address)
        );
    }

    #[test]
    #[should_panic(expected = "Edge must exist.")]
    fn test_invalid_edge_endpoints() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);

        let _ = state.edge_endpoints(EdgeAddress::new(999));
    }

    #[test]
    fn test_outgoing_edge_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let edge_index = graphrecord.edge_indices().next().unwrap();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();

        assert_eq!(
            vec![edge_address],
            state
                .outgoing_edge_addresses(lorem_address)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_incoming_edge_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let edge_index = graphrecord.edge_indices().next().unwrap();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let ipsum_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();

        assert_eq!(
            vec![edge_address],
            state
                .incoming_edge_addresses(ipsum_address)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_incident_edge_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let edge_index = graphrecord.edge_indices().next().unwrap();
        let edge_address = state.resolve_edge_address(&edge_index).unwrap();
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        let ipsum_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();

        assert_eq!(
            vec![edge_address],
            state
                .incident_edge_addresses(lorem_address)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![edge_address],
            state
                .incident_edge_addresses(ipsum_address)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_outgoing_neighbor_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        let ipsum_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();

        assert_eq!(
            vec![ipsum_address],
            state
                .outgoing_neighbor_addresses(lorem_address)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_incoming_neighbor_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        let ipsum_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();

        assert_eq!(
            vec![lorem_address],
            state
                .incoming_neighbor_addresses(ipsum_address)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_neighbor_addresses() {
        let graphrecord = create_graphrecord();
        let state = StateView::of(&graphrecord);
        let lorem_address = state
            .resolve_node_address(&NodeIndex::from("lorem"))
            .unwrap();
        let ipsum_address = state
            .resolve_node_address(&NodeIndex::from("ipsum"))
            .unwrap();

        assert_eq!(
            vec![ipsum_address],
            state.neighbor_addresses(lorem_address).collect::<Vec<_>>()
        );
    }
}
