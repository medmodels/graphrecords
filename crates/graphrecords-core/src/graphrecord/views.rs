use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{
        GraphRecord,
        datatypes::{
            AttributeName, AttributeNameView, EdgeDirection, EdgeIndex, GroupIndex, GroupIndexView,
            NodeIndexView, ValueView,
        },
        state::{EdgeAddress, GroupAddress, NodeAddress},
        state_view::StateView,
    },
};
use graphrecords_utils::distinct::Distinct;
use std::{fmt, iter::FusedIterator};

#[derive(Clone, Copy)]
pub struct NodeView<'a> {
    graphrecord: &'a GraphRecord,
    address: NodeAddress,
}

impl<'a> NodeView<'a> {
    pub(crate) fn new(
        graphrecord: &'a GraphRecord,
        node_index: NodeIndexView<'_>,
    ) -> GraphRecordResult<Self> {
        let address = StateView::of(graphrecord)
            .resolve_node_address(node_index.clone())
            .ok_or_else(|| GraphRecordError::NodeNotFound {
                node_index: node_index.into(),
            })?;

        Ok(Self {
            graphrecord,
            address,
        })
    }

    #[must_use]
    pub fn index(&self) -> NodeIndexView<'a> {
        StateView::of(self.graphrecord).node_index(self.address)
    }

    #[must_use]
    pub fn attribute<'b>(
        &self,
        attribute_name: impl Into<AttributeNameView<'b>>,
    ) -> Option<ValueView<'a>> {
        let state = StateView::of(self.graphrecord);
        let attribute_address = state.resolve_node_attribute_address(attribute_name)?;

        state.node_attribute(self.address, attribute_address)
    }

    pub fn attributes(&self) -> impl Iterator<Item = (&'a AttributeName, ValueView<'a>)> {
        let state = StateView::of(self.graphrecord);
        let address = self.address;

        state
            .node_attribute_addresses()
            .filter_map(move |attribute_address| {
                state
                    .node_attribute(address, attribute_address)
                    .map(|value| (state.node_attribute_name(attribute_address), value))
            })
    }

    pub fn groups(&self) -> impl Iterator<Item = &'a GroupIndex> {
        let state = StateView::of(self.graphrecord);

        state
            .node_group_addresses(self.address)
            .map(move |group_address| state.group_index(group_address))
    }

    pub fn edges(&self, direction: EdgeDirection) -> impl Iterator<Item = EdgeIndex> + use<'a> {
        let state = StateView::of(self.graphrecord);
        let address = self.address;

        let edge_addresses: Distinct<EdgeAddress> = match direction {
            EdgeDirection::Outgoing => state.outgoing_edge_addresses(address).collect(),
            EdgeDirection::Incoming => state.incoming_edge_addresses(address).collect(),
            EdgeDirection::Both => state.incident_edge_addresses(address).collect(),
        };

        edge_addresses
            .into_iter()
            .map(move |edge_address| state.edge_index(edge_address))
    }

    pub fn neighbors(&self, direction: EdgeDirection) -> impl Iterator<Item = NodeIndexView<'a>> {
        let state = StateView::of(self.graphrecord);
        let address = self.address;

        let neighbor_addresses: Vec<NodeAddress> = match direction {
            EdgeDirection::Outgoing => state.outgoing_neighbor_addresses(address).collect(),
            EdgeDirection::Incoming => state.incoming_neighbor_addresses(address).collect(),
            EdgeDirection::Both => state.neighbor_addresses(address).collect(),
        };

        neighbor_addresses
            .into_iter()
            .map(move |neighbor_address| state.node_index(neighbor_address))
            .collect::<Distinct<_>>()
            .into_iter()
    }

    #[must_use]
    pub fn degree(&self, direction: EdgeDirection) -> usize {
        let state = StateView::of(self.graphrecord);
        let address = self.address;

        match direction {
            EdgeDirection::Outgoing => state.outgoing_edge_addresses(address).count(),
            EdgeDirection::Incoming => state.incoming_edge_addresses(address).count(),
            EdgeDirection::Both => state.incident_edge_addresses(address).count(),
        }
    }

    pub fn edges_to<'b>(
        &self,
        target: impl Into<NodeIndexView<'b>>,
        direction: EdgeDirection,
    ) -> GraphRecordResult<ConnectingEdges> {
        let target = target.into();
        let state = StateView::of(self.graphrecord);
        let target_address = state.resolve_node_address(target.clone()).ok_or_else(|| {
            GraphRecordError::NodeNotFound {
                node_index: target.into(),
            }
        })?;
        let source_address = self.address;

        let edge_addresses: Distinct<EdgeAddress> = match direction {
            EdgeDirection::Outgoing => state.outgoing_edge_addresses(source_address).collect(),
            EdgeDirection::Incoming => state.incoming_edge_addresses(source_address).collect(),
            EdgeDirection::Both => state.incident_edge_addresses(source_address).collect(),
        };

        let edge_indices: Vec<EdgeIndex> = edge_addresses
            .into_iter()
            .filter(|&edge_address| {
                let (edge_source, edge_target) = state.edge_endpoints(edge_address);
                let other_endpoint = if edge_source == source_address {
                    edge_target
                } else {
                    edge_source
                };

                other_endpoint == target_address
            })
            .map(|edge_address| state.edge_index(edge_address))
            .collect();

        Ok(ConnectingEdges::new(edge_indices))
    }
}

impl fmt::Debug for NodeView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("NodeView")
            .field(&self.index())
            .finish()
    }
}

pub struct ConnectingEdges(std::vec::IntoIter<EdgeIndex>);

impl ConnectingEdges {
    fn new(edge_indices: Vec<EdgeIndex>) -> Self {
        Self(edge_indices.into_iter())
    }
}

impl Iterator for ConnectingEdges {
    type Item = EdgeIndex;

    fn next(&mut self) -> Option<EdgeIndex> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for ConnectingEdges {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl DoubleEndedIterator for ConnectingEdges {
    fn next_back(&mut self) -> Option<EdgeIndex> {
        self.0.next_back()
    }
}

impl FusedIterator for ConnectingEdges {}

#[derive(Clone, Copy)]
pub struct EdgeView<'a> {
    graphrecord: &'a GraphRecord,
    address: EdgeAddress,
}

impl<'a> EdgeView<'a> {
    pub(crate) fn new(
        graphrecord: &'a GraphRecord,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<Self> {
        let address = StateView::of(graphrecord)
            .resolve_edge_address(edge_index)
            .ok_or(GraphRecordError::EdgeNotFound {
                edge_index: *edge_index,
            })?;

        Ok(Self {
            graphrecord,
            address,
        })
    }

    #[must_use]
    pub fn index(&self) -> EdgeIndex {
        StateView::of(self.graphrecord).edge_index(self.address)
    }

    #[must_use]
    pub fn source(&self) -> NodeIndexView<'a> {
        let state = StateView::of(self.graphrecord);

        state.node_index(state.edge_endpoints(self.address).0)
    }

    #[must_use]
    pub fn target(&self) -> NodeIndexView<'a> {
        let state = StateView::of(self.graphrecord);

        state.node_index(state.edge_endpoints(self.address).1)
    }

    #[must_use]
    pub fn attribute<'b>(
        &self,
        attribute_name: impl Into<AttributeNameView<'b>>,
    ) -> Option<ValueView<'a>> {
        let state = StateView::of(self.graphrecord);
        let attribute_address = state.resolve_edge_attribute_address(attribute_name)?;

        state.edge_attribute(self.address, attribute_address)
    }

    pub fn attributes(&self) -> impl Iterator<Item = (&'a AttributeName, ValueView<'a>)> {
        let state = StateView::of(self.graphrecord);
        let address = self.address;

        state
            .edge_attribute_addresses()
            .filter_map(move |attribute_address| {
                state
                    .edge_attribute(address, attribute_address)
                    .map(|value| (state.edge_attribute_name(attribute_address), value))
            })
    }

    pub fn groups(&self) -> impl Iterator<Item = &'a GroupIndex> {
        let state = StateView::of(self.graphrecord);

        state
            .edge_group_addresses(self.address)
            .map(move |group_address| state.group_index(group_address))
    }
}

impl fmt::Debug for EdgeView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("EdgeView")
            .field(&self.index())
            .finish()
    }
}

#[derive(Clone, Copy)]
pub struct GroupView<'a> {
    graphrecord: &'a GraphRecord,
    address: GroupAddress,
}

impl<'a> GroupView<'a> {
    pub(crate) fn new(
        graphrecord: &'a GraphRecord,
        group_index: GroupIndexView<'_>,
    ) -> GraphRecordResult<Self> {
        let address = StateView::of(graphrecord)
            .resolve_group_address(group_index.clone())
            .ok_or_else(|| GraphRecordError::GroupNotFound {
                group_index: group_index.into(),
            })?;

        Ok(Self {
            graphrecord,
            address,
        })
    }

    #[must_use]
    pub fn index(&self) -> &'a GroupIndex {
        StateView::of(self.graphrecord).group_index(self.address)
    }

    pub fn nodes(&self) -> impl Iterator<Item = NodeIndexView<'a>> {
        let state = StateView::of(self.graphrecord);

        state
            .group_node_addresses(self.address)
            .map(move |node_address| state.node_index(node_address))
    }

    pub fn edges(&self) -> impl Iterator<Item = EdgeIndex> + use<'a> {
        let state = StateView::of(self.graphrecord);

        state
            .group_edge_addresses(self.address)
            .map(move |edge_address| state.edge_index(edge_address))
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        StateView::of(self.graphrecord).group_node_count(self.address)
    }

    #[must_use]
    pub fn edge_count(&self) -> usize {
        StateView::of(self.graphrecord).group_edge_count(self.address)
    }
}

impl fmt::Debug for GroupView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("GroupView")
            .field(self.index())
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::{EdgeView, GroupView, NodeView};
    use crate::{
        errors::GraphRecordError,
        graphrecord::{
            AttributeMap, AttributeName, EdgeDirection, EdgeIndex, GraphRecord, GroupIndex,
            Identifier, NodeIndexView, Value,
        },
    };
    use std::collections::HashSet;

    fn create_graphrecord_with_two_nodes() -> GraphRecord {
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
    }

    fn create_graphrecord_with_one_edge() -> (GraphRecord, EdgeIndex) {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_edges_in_group(
                vec![(
                    "lorem",
                    "ipsum",
                    AttributeMap::from([("sed".into(), true.into())]),
                )],
                "dolor",
            )
            .unwrap();
        let edge_index = graphrecord.edge_indices().next().unwrap();

        (graphrecord, edge_index)
    }

    fn create_graphrecord_with_directed_edges() -> (GraphRecord, Vec<EdgeIndex>) {
        let graphrecord = create_graphrecord_with_two_nodes()
            .add_node("amet", AttributeMap::new())
            .unwrap()
            .add_edges_in_group(
                vec![
                    ("lorem", "ipsum", AttributeMap::new()),
                    ("lorem", "ipsum", AttributeMap::new()),
                    ("amet", "lorem", AttributeMap::new()),
                    ("lorem", "lorem", AttributeMap::new()),
                ],
                "dolor",
            )
            .unwrap();
        let edge_indices = graphrecord.edge_indices().collect();

        (graphrecord, edge_indices)
    }

    #[test]
    fn test_node_view_new() {
        let graphrecord = create_graphrecord_with_two_nodes();

        let lorem = NodeView::new(&graphrecord, "lorem".into()).unwrap();
        let ipsum = NodeView::new(&graphrecord, "ipsum".into()).unwrap();

        assert_eq!(
            NodeIndexView::from(&Identifier::from("lorem")),
            lorem.index()
        );
        assert_eq!(
            NodeIndexView::from(&Identifier::from("ipsum")),
            ipsum.index()
        );
    }

    #[test]
    fn test_invalid_node_view_new() {
        let graphrecord = create_graphrecord_with_two_nodes();

        let result = NodeView::new(&graphrecord, "dolor".into());

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeNotFound { node_index }
                if node_index == "dolor".into()
        )));
    }

    #[test]
    fn test_node_view_index() {
        let graphrecord = create_graphrecord_with_two_nodes();
        let node_view = NodeView::new(&graphrecord, "lorem".into()).unwrap();

        assert_eq!(
            NodeIndexView::from(&Identifier::from("lorem")),
            node_view.index()
        );
    }

    #[test]
    fn test_node_view_attribute() {
        let graphrecord = create_graphrecord_with_two_nodes();
        let node_view = NodeView::new(&graphrecord, "lorem".into()).unwrap();

        assert_eq!(Some(42.into()), node_view.attribute("sed").map(Value::from));
        assert_eq!(None, node_view.attribute("dolor"));
    }

    #[test]
    fn test_node_view_attributes() {
        let graphrecord = create_graphrecord_with_two_nodes();
        let node_view = NodeView::new(&graphrecord, "lorem".into()).unwrap();

        let attributes: Vec<_> = node_view
            .attributes()
            .map(|(name, value)| (name.clone(), Value::from(value)))
            .collect();

        assert_eq!(
            vec![(AttributeName::from("sed"), Value::from(42))],
            attributes
        );
    }

    #[test]
    fn test_node_view_groups() {
        let graphrecord = create_graphrecord_with_two_nodes();
        let node_view = NodeView::new(&graphrecord, "lorem".into()).unwrap();

        assert_eq!(
            vec![&GroupIndex::from("dolor")],
            node_view.groups().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_node_view_edges() {
        let (graphrecord, edge_indices) = create_graphrecord_with_directed_edges();
        let node_view = NodeView::new(&graphrecord, "lorem".into()).unwrap();

        assert_eq!(
            HashSet::from([edge_indices[0], edge_indices[1], edge_indices[3]]),
            node_view
                .edges(EdgeDirection::Outgoing)
                .collect::<HashSet<_>>()
        );
        assert_eq!(
            HashSet::from([edge_indices[2], edge_indices[3]]),
            node_view
                .edges(EdgeDirection::Incoming)
                .collect::<HashSet<_>>()
        );

        let both_edges: Vec<_> = node_view.edges(EdgeDirection::Both).collect();

        assert_eq!(4, both_edges.len());
        assert_eq!(
            1,
            both_edges
                .iter()
                .filter(|&&edge_index| edge_index == edge_indices[3])
                .count()
        );
    }

    #[test]
    fn test_node_view_neighbors() {
        let (graphrecord, _) = create_graphrecord_with_directed_edges();
        let node_view = NodeView::new(&graphrecord, "lorem".into()).unwrap();

        assert_eq!(
            vec![
                NodeIndexView::from(&Identifier::from("lorem")),
                NodeIndexView::from(&Identifier::from("ipsum")),
            ],
            node_view
                .neighbors(EdgeDirection::Outgoing)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![
                NodeIndexView::from(&Identifier::from("lorem")),
                NodeIndexView::from(&Identifier::from("amet")),
            ],
            node_view
                .neighbors(EdgeDirection::Incoming)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![
                NodeIndexView::from(&Identifier::from("lorem")),
                NodeIndexView::from(&Identifier::from("ipsum")),
                NodeIndexView::from(&Identifier::from("amet")),
            ],
            node_view.neighbors(EdgeDirection::Both).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_node_view_degree() {
        let (graphrecord, _) = create_graphrecord_with_directed_edges();
        let node_view = NodeView::new(&graphrecord, "lorem".into()).unwrap();

        assert_eq!(3, node_view.degree(EdgeDirection::Outgoing));
        assert_eq!(2, node_view.degree(EdgeDirection::Incoming));
        assert_eq!(5, node_view.degree(EdgeDirection::Both));
    }

    #[test]
    fn test_node_view_edges_to() {
        let (graphrecord, edge_indices) = create_graphrecord_with_directed_edges();
        let node_view = NodeView::new(&graphrecord, "lorem".into()).unwrap();

        assert_eq!(
            HashSet::from([edge_indices[0], edge_indices[1]]),
            node_view
                .edges_to("ipsum", EdgeDirection::Outgoing)
                .unwrap()
                .collect::<HashSet<_>>()
        );
        assert_eq!(
            0,
            node_view
                .edges_to("ipsum", EdgeDirection::Incoming)
                .unwrap()
                .count()
        );
        assert_eq!(
            HashSet::from([edge_indices[2]]),
            node_view
                .edges_to("amet", EdgeDirection::Incoming)
                .unwrap()
                .collect::<HashSet<_>>()
        );
        let self_loops = node_view.edges_to("lorem", EdgeDirection::Both).unwrap();

        assert_eq!(1, self_loops.len());
        assert_eq!(vec![edge_indices[3]], self_loops.rev().collect::<Vec<_>>());
    }

    #[test]
    fn test_invalid_node_view_edges_to() {
        let (graphrecord, _) = create_graphrecord_with_directed_edges();
        let node_view = NodeView::new(&graphrecord, "lorem".into()).unwrap();

        let result = node_view.edges_to("dolor", EdgeDirection::Outgoing);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodeNotFound { node_index }
                if node_index == "dolor".into()
        )));
    }

    #[test]
    fn test_edge_view_new() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();

        let edge_view = EdgeView::new(&graphrecord, &edge_index).unwrap();

        assert_eq!(edge_index, edge_view.index());
    }

    #[test]
    fn test_invalid_edge_view_new() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let removed = graphrecord.remove_edges(vec![edge_index]).unwrap();

        let result = EdgeView::new(&removed, &edge_index);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::EdgeNotFound {
                edge_index: found_edge_index
            } if found_edge_index == edge_index
        )));
    }

    #[test]
    fn test_edge_view_index() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let edge_view = EdgeView::new(&graphrecord, &edge_index).unwrap();

        assert_eq!(edge_index, edge_view.index());
    }

    #[test]
    fn test_edge_view_source() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let edge_view = EdgeView::new(&graphrecord, &edge_index).unwrap();

        assert_eq!(
            NodeIndexView::from(&Identifier::from("lorem")),
            edge_view.source()
        );
    }

    #[test]
    fn test_edge_view_target() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let edge_view = EdgeView::new(&graphrecord, &edge_index).unwrap();

        assert_eq!(
            NodeIndexView::from(&Identifier::from("ipsum")),
            edge_view.target()
        );
    }

    #[test]
    fn test_edge_view_attribute() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let edge_view = EdgeView::new(&graphrecord, &edge_index).unwrap();

        assert_eq!(
            Some(true.into()),
            edge_view.attribute("sed").map(Value::from)
        );
        assert_eq!(None, edge_view.attribute("dolor"));
    }

    #[test]
    fn test_edge_view_attributes() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let edge_view = EdgeView::new(&graphrecord, &edge_index).unwrap();

        let attributes: Vec<_> = edge_view
            .attributes()
            .map(|(name, value)| (name.clone(), Value::from(value)))
            .collect();

        assert_eq!(
            vec![(AttributeName::from("sed"), Value::from(true))],
            attributes
        );
    }

    #[test]
    fn test_edge_view_groups() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let edge_view = EdgeView::new(&graphrecord, &edge_index).unwrap();

        assert_eq!(
            vec![&GroupIndex::from("dolor")],
            edge_view.groups().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_group_view_new() {
        let graphrecord = create_graphrecord_with_two_nodes();

        let group_view = GroupView::new(&graphrecord, "dolor".into()).unwrap();

        assert_eq!(&GroupIndex::from("dolor"), group_view.index());
    }

    #[test]
    fn test_invalid_group_view_new() {
        let graphrecord = create_graphrecord_with_two_nodes();

        let result = GroupView::new(&graphrecord, "sit".into());

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::GroupNotFound { group_index }
                if group_index == "sit".into()
        )));
    }

    #[test]
    fn test_group_view_index() {
        let graphrecord = create_graphrecord_with_two_nodes();
        let group_view = GroupView::new(&graphrecord, "dolor".into()).unwrap();

        assert_eq!(&GroupIndex::from("dolor"), group_view.index());
    }

    #[test]
    fn test_group_view_nodes() {
        let (graphrecord, _) = create_graphrecord_with_one_edge();
        let group_view = GroupView::new(&graphrecord, "dolor".into()).unwrap();

        assert_eq!(
            HashSet::from([
                NodeIndexView::from(&Identifier::from("lorem")),
                NodeIndexView::from(&Identifier::from("ipsum")),
            ]),
            group_view.nodes().collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_group_view_edges() {
        let (graphrecord, edge_index) = create_graphrecord_with_one_edge();
        let group_view = GroupView::new(&graphrecord, "dolor".into()).unwrap();

        assert_eq!(vec![edge_index], group_view.edges().collect::<Vec<_>>());
    }

    #[test]
    fn test_group_view_node_count() {
        let (graphrecord, _) = create_graphrecord_with_one_edge();
        let group_view = GroupView::new(&graphrecord, "dolor".into()).unwrap();

        assert_eq!(2, group_view.node_count());
    }

    #[test]
    fn test_group_view_edge_count() {
        let (graphrecord, _) = create_graphrecord_with_one_edge();
        let group_view = GroupView::new(&graphrecord, "dolor".into()).unwrap();

        assert_eq!(1, group_view.edge_count());
    }
}
