mod edge;
mod node;

#[cfg(feature = "safe-handles")]
use super::intern_table::{Handle, HandleKind, fresh_graph_id};
use super::{
    GraphRecordAttribute, GraphRecordValue,
    group_mapping::GroupMapping,
    intern_table::{
        AttributeNameKind, AttributesView, GroupKind, HandleAttributes, InternTable,
        NodeHandle, NodeIndexKind,
    },
};
use crate::errors::GraphError;
use edge::Edge;
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use node::Node;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type NodeIndex = GraphRecordAttribute;
pub type EdgeIndex = u32;
pub type Attributes = HashMap<GraphRecordAttribute, GraphRecordValue>;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(super) struct Graph {
    #[cfg(feature = "safe-handles")]
    pub(crate) id: u32,
    pub(crate) nodes: GrHashMap<NodeHandle, Node>,
    pub(crate) edges: GrHashMap<EdgeIndex, Edge>,
    pub(crate) edge_index_counter: u32,
    pub(crate) node_index_table: InternTable<NodeIndexKind>,
    pub(crate) attribute_name_table: InternTable<AttributeNameKind>,
    pub(crate) group_name_table: InternTable<GroupKind>,
}

#[cfg(not(feature = "safe-handles"))]
fn new_intern_table<K: super::intern_table::HandleKind>(capacity: usize) -> InternTable<K> {
    InternTable::with_capacity(capacity)
}

#[cfg(feature = "safe-handles")]
fn new_intern_table<K: super::intern_table::HandleKind>(
    capacity: usize,
    graph_id: u32,
) -> InternTable<K> {
    InternTable::with_capacity(capacity, graph_id)
}

impl Graph {
    pub fn new() -> Self {
        Self::with_capacity(0, 0)
    }

    pub fn with_capacity(node_capacity: usize, edge_capacity: usize) -> Self {
        #[cfg(feature = "safe-handles")]
        let graph_id = fresh_graph_id();

        #[cfg(not(feature = "safe-handles"))]
        let (node_index_table, attribute_name_table, group_name_table) = (
            new_intern_table(node_capacity),
            new_intern_table(0),
            new_intern_table(0),
        );

        #[cfg(feature = "safe-handles")]
        let (node_index_table, attribute_name_table, group_name_table) = (
            new_intern_table(node_capacity, graph_id),
            new_intern_table(0, graph_id),
            new_intern_table(0, graph_id),
        );

        Self {
            #[cfg(feature = "safe-handles")]
            id: graph_id,
            nodes: GrHashMap::with_capacity(node_capacity),
            edges: GrHashMap::with_capacity(edge_capacity),
            edge_index_counter: 0,
            node_index_table,
            attribute_name_table,
            group_name_table,
        }
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.edges.clear();

        self.edge_index_counter = 0;

        self.node_index_table.clear();
        self.attribute_name_table.clear();
        self.group_name_table.clear();

        #[cfg(feature = "safe-handles")]
        {
            self.id = fresh_graph_id();

            self.node_index_table.set_graph_id(self.id);
            self.attribute_name_table.set_graph_id(self.id);
            self.group_name_table.set_graph_id(self.id);
        }
    }

    #[cfg(feature = "safe-handles")]
    pub(crate) fn validate_handle<K: HandleKind>(
        &self,
        handle: Handle<K>,
    ) -> Result<(), GraphError> {
        if handle.graph_id() != self.id {
            return Err(GraphError::StaleHandle(format!(
                "Handle from graph {} used on graph {}",
                handle.graph_id(),
                self.id
            )));
        }

        Ok(())
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub(crate) fn intern_attributes(&mut self, attributes: Attributes) -> HandleAttributes {
        attributes
            .into_iter()
            .map(|(name, value)| (self.attribute_name_table.intern_owned(name), value))
            .collect()
    }

    pub(crate) fn resolve_handle_attributes(&self, attributes: &HandleAttributes) -> Attributes {
        attributes
            .iter()
            .map(|(handle, value)| {
                (
                    self.attribute_name_table.resolve(*handle).clone(),
                    value.clone(),
                )
            })
            .collect()
    }

    pub fn add_node(
        &mut self,
        node_index: NodeIndex,
        attributes: Attributes,
    ) -> Result<NodeHandle, GraphError> {
        if let Some(existing_handle) = self.node_index_table.get(&node_index)
            && self.nodes.contains_key(&existing_handle)
        {
            return Err(GraphError::AssertionError(format!(
                "Node with index {node_index} already exists"
            )));
        }

        let handle = self.node_index_table.intern_owned(node_index);
        let handle_attributes = self.intern_attributes(attributes);
        let node = Node::new(handle_attributes);

        self.nodes.insert(handle, node);

        Ok(handle)
    }

    pub fn remove_node(
        &mut self,
        handle: NodeHandle,
        group_mapping: &mut GroupMapping,
    ) -> Result<Attributes, GraphError> {
        let node = self.nodes.remove(&handle).ok_or_else(|| {
            GraphError::IndexError(format!("Cannot find node for handle {handle:?}"))
        })?;

        group_mapping.remove_node(handle);

        let edge_indices = node
            .outgoing_edge_indices
            .union(&node.incoming_edge_indices);

        for edge_index in edge_indices {
            group_mapping.remove_edge(edge_index);

            let edge = self.edges.remove(edge_index).expect("Edge must exist");

            match (
                edge.source_node_handle == handle,
                edge.target_node_handle == handle,
            ) {
                (true, true) => {}
                (true, false) => {
                    self.nodes
                        .get_mut(&edge.target_node_handle)
                        .expect("Node must exist")
                        .incoming_edge_indices
                        .remove(edge_index);
                }
                (false, true) => {
                    self.nodes
                        .get_mut(&edge.source_node_handle)
                        .expect("Node must exist")
                        .outgoing_edge_indices
                        .remove(edge_index);
                }
                (false, false) => unreachable!(),
            }
        }

        Ok(self.resolve_handle_attributes(&node.attributes))
    }

    pub fn contains_node(&self, handle: NodeHandle) -> bool {
        self.nodes.contains_key(&handle)
    }

    pub fn add_edge(
        &mut self,
        source_handle: NodeHandle,
        target_handle: NodeHandle,
        attributes: Attributes,
    ) -> Result<EdgeIndex, GraphError> {
        if !self.nodes.contains_key(&source_handle) {
            return Err(GraphError::IndexError(format!(
                "Cannot find node for handle {source_handle:?}"
            )));
        }

        if !self.nodes.contains_key(&target_handle) {
            return Err(GraphError::IndexError(format!(
                "Cannot find node for handle {target_handle:?}"
            )));
        }

        let edge_index = self.edge_index_counter;
        self.edge_index_counter += 1;

        let handle_attributes = self.intern_attributes(attributes);

        self.nodes
            .get_mut(&source_handle)
            .expect("Node must exist")
            .outgoing_edge_indices
            .insert(edge_index);

        self.nodes
            .get_mut(&target_handle)
            .expect("Node must exist")
            .incoming_edge_indices
            .insert(edge_index);

        let edge = Edge::new(handle_attributes, source_handle, target_handle);

        self.edges.insert(edge_index, edge);

        Ok(edge_index)
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn remove_edge(&mut self, edge_index: &EdgeIndex) -> Result<Attributes, GraphError> {
        let edge = self.edges.remove(edge_index).ok_or_else(|| {
            GraphError::IndexError(format!("Cannot find edge with index {edge_index}"))
        })?;

        self.nodes
            .get_mut(&edge.target_node_handle)
            .expect("Node must exist")
            .incoming_edge_indices
            .remove(edge_index);

        self.nodes
            .get_mut(&edge.source_node_handle)
            .expect("Node must exist")
            .outgoing_edge_indices
            .remove(edge_index);

        Ok(self.resolve_handle_attributes(&edge.attributes))
    }

    pub fn node_attributes(
        &self,
        handle: NodeHandle,
    ) -> Result<AttributesView<'_>, GraphError> {
        let node = self.nodes.get(&handle).ok_or_else(|| {
            GraphError::IndexError(format!("Cannot find node for handle {handle:?}"))
        })?;

        Ok(AttributesView::new(
            &self.attribute_name_table,
            &node.attributes,
        ))
    }

    pub fn replace_node_attributes(
        &mut self,
        handle: NodeHandle,
        attributes: Attributes,
    ) -> Result<(), GraphError> {
        let handle_attributes = self.intern_attributes(attributes);
        let node = self.nodes.get_mut(&handle).ok_or_else(|| {
            GraphError::IndexError(format!("Cannot find node for handle {handle:?}"))
        })?;

        node.attributes = handle_attributes;

        Ok(())
    }

    pub fn node_indices(&self) -> impl Iterator<Item = &NodeIndex> {
        self.nodes
            .keys()
            .map(|handle| self.node_index_table.resolve(*handle))
    }

    pub fn node_handles(&self) -> impl Iterator<Item = NodeHandle> + use<'_> {
        self.nodes.keys().copied()
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn edge_attributes(
        &self,
        edge_index: &EdgeIndex,
    ) -> Result<AttributesView<'_>, GraphError> {
        let edge = self.edges.get(edge_index).ok_or_else(|| {
            GraphError::IndexError(format!("Cannot find edge with index {edge_index}"))
        })?;
        Ok(AttributesView::new(
            &self.attribute_name_table,
            &edge.attributes,
        ))
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(crate) fn edge_handle_attributes_mut(
        &mut self,
        edge_index: &EdgeIndex,
    ) -> Result<&mut HandleAttributes, GraphError> {
        Ok(&mut self
            .edges
            .get_mut(edge_index)
            .ok_or_else(|| {
                GraphError::IndexError(format!("Cannot find edge with index {edge_index}"))
            })?
            .attributes)
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn replace_edge_attributes(
        &mut self,
        edge_index: &EdgeIndex,
        attributes: Attributes,
    ) -> Result<(), GraphError> {
        let handle_attributes = self.intern_attributes(attributes);
        let edge_attributes = self.edge_handle_attributes_mut(edge_index)?;
        *edge_attributes = handle_attributes;
        Ok(())
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn edge_endpoint_handles(
        &self,
        edge_index: &EdgeIndex,
    ) -> Result<(NodeHandle, NodeHandle), GraphError> {
        let edge = self.edges.get(edge_index).ok_or_else(|| {
            GraphError::IndexError(format!("Cannot find edge with index {edge_index}"))
        })?;

        Ok((edge.source_node_handle, edge.target_node_handle))
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn edge_endpoints(
        &self,
        edge_index: &EdgeIndex,
    ) -> Result<(&NodeIndex, &NodeIndex), GraphError> {
        let (source_handle, target_handle) = self.edge_endpoint_handles(edge_index)?;

        Ok((
            self.node_index_table.resolve(source_handle),
            self.node_index_table.resolve(target_handle),
        ))
    }

    pub fn outgoing_edges(
        &self,
        handle: NodeHandle,
    ) -> Result<impl Iterator<Item = &EdgeIndex> + use<'_>, GraphError> {
        Ok(self
            .nodes
            .get(&handle)
            .ok_or_else(|| {
                GraphError::IndexError(format!("Cannot find node for handle {handle:?}"))
            })?
            .outgoing_edge_indices
            .iter())
    }

    pub fn incoming_edges(
        &self,
        handle: NodeHandle,
    ) -> Result<impl Iterator<Item = &EdgeIndex> + use<'_>, GraphError> {
        Ok(self
            .nodes
            .get(&handle)
            .ok_or_else(|| {
                GraphError::IndexError(format!("Cannot find node for handle {handle:?}"))
            })?
            .incoming_edge_indices
            .iter())
    }

    pub fn edge_indices(&self) -> impl Iterator<Item = &EdgeIndex> {
        self.edges.keys()
    }

    pub fn edges_connecting<SN, TN>(
        &self,
        source_handles: SN,
        target_handles: TN,
    ) -> impl Iterator<Item = &EdgeIndex>
    where
        SN: IntoIterator<Item = NodeHandle>,
        TN: IntoIterator<Item = NodeHandle>,
    {
        let target_set: GrHashSet<NodeHandle> = target_handles.into_iter().collect();

        let mut result = Vec::new();

        for source_handle in source_handles {
            let Some(node) = self.nodes.get(&source_handle) else {
                continue;
            };

            for edge_index in &node.outgoing_edge_indices {
                let edge = self.edges.get(edge_index).expect("Edge must exist");

                if target_set.contains(&edge.target_node_handle) {
                    result.push(edge_index);
                }
            }
        }

        result.into_iter()
    }

    pub fn edges_connecting_undirected<SN, TN>(
        &self,
        first_handles: SN,
        second_handles: TN,
    ) -> impl Iterator<Item = &EdgeIndex>
    where
        SN: IntoIterator<Item = NodeHandle>,
        TN: IntoIterator<Item = NodeHandle>,
    {
        let first_set: GrHashSet<NodeHandle> = first_handles.into_iter().collect();
        let second_set: GrHashSet<NodeHandle> = second_handles.into_iter().collect();

        let mut result = GrHashSet::new();

        self.collect_edges_between(&first_set, &second_set, &mut result);
        self.collect_edges_between(&second_set, &first_set, &mut result);

        result.into_iter()
    }

    fn collect_edges_between<'a>(
        &'a self,
        sources: &GrHashSet<NodeHandle>,
        targets: &GrHashSet<NodeHandle>,
        result: &mut GrHashSet<&'a EdgeIndex>,
    ) {
        for source_handle in sources {
            let Some(node) = self.nodes.get(source_handle) else {
                continue;
            };

            for edge_index in &node.outgoing_edge_indices {
                let edge = self.edges.get(edge_index).expect("Edge must exist");

                if targets.contains(&edge.target_node_handle) {
                    result.insert(edge_index);
                }
            }

            for edge_index in &node.incoming_edge_indices {
                let edge = self.edges.get(edge_index).expect("Edge must exist");

                if targets.contains(&edge.source_node_handle) {
                    result.insert(edge_index);
                }
            }
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn contains_edge(&self, edge_index: &EdgeIndex) -> bool {
        self.edges.contains_key(edge_index)
    }

    pub fn outgoing_neighbor_handles(
        &self,
        handle: NodeHandle,
    ) -> Result<impl Iterator<Item = NodeHandle> + use<'_>, GraphError> {
        let node = self.nodes.get(&handle).ok_or_else(|| {
            GraphError::IndexError(format!("Cannot find node for handle {handle:?}"))
        })?;

        Ok(node.outgoing_edge_indices.iter().map(|edge_index| {
            self.edges
                .get(edge_index)
                .expect("Edge must exist")
                .target_node_handle
        }))
    }

    pub fn outgoing_neighbors(
        &self,
        handle: NodeHandle,
    ) -> Result<impl Iterator<Item = &NodeIndex> + use<'_>, GraphError> {
        Ok(self
            .outgoing_neighbor_handles(handle)?
            .map(|handle| self.node_index_table.resolve(handle)))
    }

    pub fn incoming_neighbor_handles(
        &self,
        handle: NodeHandle,
    ) -> Result<impl Iterator<Item = NodeHandle> + use<'_>, GraphError> {
        let node = self.nodes.get(&handle).ok_or_else(|| {
            GraphError::IndexError(format!("Cannot find node for handle {handle:?}"))
        })?;

        Ok(node.incoming_edge_indices.iter().map(|edge_index| {
            self.edges
                .get(edge_index)
                .expect("Edge must exist")
                .source_node_handle
        }))
    }

    pub fn incoming_neighbors(
        &self,
        handle: NodeHandle,
    ) -> Result<impl Iterator<Item = &NodeIndex> + use<'_>, GraphError> {
        Ok(self
            .incoming_neighbor_handles(handle)?
            .map(|handle| self.node_index_table.resolve(handle)))
    }

    pub fn neighbor_handles(
        &self,
        handle: NodeHandle,
    ) -> Result<impl Iterator<Item = NodeHandle> + use<'_>, GraphError> {
        let node = self.nodes.get(&handle).ok_or_else(|| {
            GraphError::IndexError(format!("Cannot find node for handle {handle:?}"))
        })?;

        Ok(node
            .outgoing_edge_indices
            .iter()
            .map(|edge_index| {
                self.edges
                    .get(edge_index)
                    .expect("Edge must exist")
                    .target_node_handle
            })
            .chain(node.incoming_edge_indices.iter().map(|edge_index| {
                self.edges
                    .get(edge_index)
                    .expect("Edge must exist")
                    .source_node_handle
            }))
            .collect::<GrHashSet<_>>()
            .into_iter())
    }

    pub fn neighbors(
        &self,
        handle: NodeHandle,
    ) -> Result<impl Iterator<Item = &NodeIndex> + use<'_>, GraphError> {
        Ok(self
            .neighbor_handles(handle)?
            .map(|handle| self.node_index_table.resolve(handle)))
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::super::intern_table::{Handle, NodeHandle};
    use super::{Attributes, Graph, NodeIndex};
    use crate::{errors::GraphError, graphrecord::group_mapping::GroupMapping};
    use std::collections::HashMap;

    fn create_nodes() -> Vec<(NodeIndex, Attributes)> {
        vec![
            (
                "0".into(),
                HashMap::from([
                    ("lorem".into(), "ipsum".into()),
                    ("dolor".into(), "sit".into()),
                ]),
            ),
            (
                "1".into(),
                HashMap::from([("amet".into(), "consectetur".into())]),
            ),
            (
                "2".into(),
                HashMap::from([("adipiscing".into(), "elit".into())]),
            ),
            ("3".into(), HashMap::new()),
        ]
    }

    fn create_edges() -> Vec<(NodeIndex, NodeIndex, Attributes)> {
        vec![
            (
                "0".into(),
                "1".into(),
                HashMap::from([
                    ("sed".into(), "do".into()),
                    ("eiusmod".into(), "tempor".into()),
                ]),
            ),
            (
                "1".into(),
                "0".into(),
                HashMap::from([
                    ("sed".into(), "do".into()),
                    ("eiusmod".into(), "tempor".into()),
                ]),
            ),
            (
                "1".into(),
                "2".into(),
                HashMap::from([("incididunt".into(), "ut".into())]),
            ),
            ("0".into(), "2".into(), HashMap::new()),
        ]
    }

    fn create_graph() -> Graph {
        let nodes = create_nodes();
        let edges = create_edges();

        let mut graph = Graph::with_capacity(nodes.len(), edges.len());

        for (node_index, attributes) in nodes {
            graph.add_node(node_index, attributes).unwrap();
        }

        for (source_node_index, target_node_index, attributes) in edges {
            let source_handle = graph.node_index_table.get(&source_node_index).unwrap();
            let target_handle = graph.node_index_table.get(&target_node_index).unwrap();
            graph
                .add_edge(source_handle, target_handle, attributes)
                .unwrap();
        }

        graph
    }

    #[test]
    fn test_clear() {
        let mut graph = create_graph();

        graph.clear();

        assert_eq!(0, graph.node_count());
        assert_eq!(0, graph.edge_count());
    }

    #[test]
    fn test_node_count() {
        let mut graph = Graph::new();

        assert_eq!(0, graph.node_count());

        graph.add_node("0".into(), HashMap::new()).unwrap();

        assert_eq!(1, graph.node_count());
    }

    #[test]
    fn test_edge_count() {
        let mut graph = Graph::new();

        let source_handle = graph.add_node("0".into(), HashMap::new()).unwrap();
        let target_handle = graph.add_node("1".into(), HashMap::new()).unwrap();

        assert_eq!(0, graph.edge_count());

        graph
            .add_edge(source_handle, target_handle, HashMap::new())
            .unwrap();

        assert_eq!(1, graph.edge_count());
    }

    #[test]
    fn test_add_node() {
        let mut graph = Graph::new();

        assert_eq!(0, graph.node_count());

        graph.add_node("0".into(), HashMap::new()).unwrap();

        assert_eq!(1, graph.node_count());
    }

    #[test]
    fn test_invalid_add_node() {
        let mut graph = create_graph();

        assert!(
            graph
                .add_node("0".into(), HashMap::new())
                .is_err_and(|e| matches!(e, GraphError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_node() {
        let mut graph = create_graph();

        assert_eq!(4, graph.node_count());

        let handle = graph.node_index_table.get(&"0".into()).unwrap();
        let attributes = graph
            .remove_node(handle, &mut GroupMapping::default())
            .unwrap();

        assert_eq!(3, graph.node_count());

        assert_eq!(create_nodes()[0].1, attributes);

        let mut graph = Graph::new();

        let node_handle = graph.add_node(0.into(), HashMap::new()).unwrap();
        graph
            .add_edge(node_handle, node_handle, HashMap::new())
            .unwrap();

        assert_eq!(1, graph.node_count());
        assert_eq!(1, graph.edge_count());

        assert!(
            graph
                .remove_node(node_handle, &mut GroupMapping::default())
                .is_ok()
        );

        assert_eq!(0, graph.node_count());
        assert_eq!(0, graph.edge_count());
    }

    #[test]
    fn test_invalid_remove_node() {
        let graph = create_graph();

        // A never-interned name has no handle to even pass in
        assert!(graph.node_index_table.get(&"50".into()).is_none());
    }

    #[test]
    fn test_contains_node() {
        let graph = create_graph();

        let handle = graph.node_index_table.get(&"0".into()).unwrap();
        assert!(graph.contains_node(handle));

        assert!(graph.node_index_table.get(&"50".into()).is_none());
    }

    #[test]
    fn test_add_edge() {
        let mut graph = create_graph();

        assert_eq!(4, graph.edge_count());

        let source_handle = graph.node_index_table.get(&"0".into()).unwrap();
        let target_handle = graph.node_index_table.get(&"3".into()).unwrap();
        graph
            .add_edge(source_handle, target_handle, HashMap::new())
            .unwrap();

        assert_eq!(5, graph.edge_count());
    }

    #[test]
    fn test_invalid_add_edge() {
        let mut graph = Graph::new();
        let valid_handle = graph.add_node(0.into(), HashMap::new()).unwrap();

        #[cfg(not(feature = "safe-handles"))]
        let missing_handle = Handle::new(u32::MAX);

        #[cfg(feature = "safe-handles")]
        let missing_handle = Handle::new(graph.id, u32::MAX);

        // Adding an edge pointing to a non-existing node should fail
        assert!(
            graph
                .add_edge(valid_handle, missing_handle, HashMap::new())
                .is_err_and(|e| matches!(e, GraphError::IndexError(_)))
        );

        // Adding an edge from a non-existing node should fail
        assert!(
            graph
                .add_edge(missing_handle, valid_handle, HashMap::new())
                .is_err_and(|e| matches!(e, GraphError::IndexError(_)))
        );
    }

    #[test]
    fn test_remove_edge() {
        let mut graph = create_graph();

        let attributes = graph.remove_edge(&0).unwrap();

        assert_eq!(3, graph.edge_count());

        assert_eq!(create_edges()[0].2, attributes);
    }

    #[test]
    fn test_invalid_remove_edge() {
        let mut graph = create_graph();

        // Removing an edge with a non-existing edge index should fail
        assert!(
            graph
                .remove_edge(&50)
                .is_err_and(|e| matches!(e, GraphError::IndexError(_)))
        );
    }

    #[test]
    fn test_node_attributes() {
        let graph = create_graph();

        let handle = graph.node_index_table.get(&"0".into()).unwrap();
        assert_eq!(&create_nodes()[0].1, graph.node_attributes(handle).unwrap());
    }

    #[test]
    fn test_invalid_node_attributes() {
        let graph = create_graph();

        // A name that was never interned has no handle at all
        assert!(graph.node_index_table.get(&"50".into()).is_none());
    }

    #[test]
    fn test_replace_node_attributes() {
        let mut graph = create_graph();

        let handle = graph.node_index_table.get(&"0".into()).unwrap();

        assert_eq!(graph.node_attributes(handle).unwrap(), create_nodes()[0].1);

        let new_attributes = HashMap::from([("0".into(), "1".into()), ("2".into(), "3".into())]);

        graph
            .replace_node_attributes(handle, new_attributes.clone())
            .unwrap();

        assert_eq!(graph.node_attributes(handle).unwrap(), new_attributes);
    }

    #[test]
    fn test_invalid_replace_node_attributes() {
        let mut graph = create_graph();

        let handle = graph.node_index_table.get(&"0".into()).unwrap();
        let mut group_mapping = GroupMapping::default();
        graph.remove_node(handle, &mut group_mapping).unwrap();

        assert!(
            graph
                .replace_node_attributes(handle, HashMap::new())
                .is_err_and(|e| matches!(e, GraphError::IndexError(_)))
        );
    }

    #[test]
    fn test_node_indices() {
        let graph = create_graph();

        let node_indices: Vec<_> = create_nodes()
            .into_iter()
            .map(|(node_index, _)| node_index)
            .collect();

        for node_index in graph.node_indices() {
            assert!(node_indices.contains(node_index));
        }
    }

    #[test]
    fn test_edge_attributes() {
        let graph = create_graph();

        assert_eq!(graph.edge_attributes(&0).unwrap(), create_edges()[0].2);
    }

    #[test]
    fn test_invalid_edge_attributes() {
        let graph = create_graph();

        // Accessing the edge attributes of a non-existing edge should fail
        assert!(
            graph
                .edge_attributes(&50)
                .is_err_and(|e| matches!(e, GraphError::IndexError(_)))
        );
    }

    #[test]
    fn test_replace_edge_attributes() {
        let mut graph = create_graph();

        assert_eq!(graph.edge_attributes(&0).unwrap(), create_edges()[0].2);

        let new_attributes = HashMap::from([("0".into(), "1".into()), ("2".into(), "3".into())]);

        graph
            .replace_edge_attributes(&0, new_attributes.clone())
            .unwrap();

        assert_eq!(graph.edge_attributes(&0).unwrap(), new_attributes);
    }

    #[test]
    fn test_invalid_replace_edge_attributes() {
        let mut graph = create_graph();

        assert!(
            graph
                .replace_edge_attributes(&50, HashMap::new())
                .is_err_and(|e| matches!(e, GraphError::IndexError(_)))
        );
    }

    #[test]
    fn test_edge_endpoints() {
        let graph = create_graph();

        let edge = &create_edges()[0];

        let (source_node_index, target_node_index) = graph.edge_endpoints(&0).unwrap();

        assert_eq!(&edge.0, source_node_index);
        assert_eq!(&edge.1, target_node_index);
    }

    #[test]
    fn test_invalid_edge_endpoints() {
        let graph = create_graph();

        assert!(
            graph
                .edge_endpoints(&50)
                .is_err_and(|e| matches!(e, GraphError::IndexError(_)))
        );
    }

    #[test]
    fn test_edge_endpoint_handles() {
        let graph = create_graph();

        let edge = &create_edges()[0];

        let (source_handle, target_handle) = graph.edge_endpoint_handles(&0).unwrap();

        assert_eq!(&edge.0, graph.node_index_table.resolve(source_handle));
        assert_eq!(&edge.1, graph.node_index_table.resolve(target_handle));
    }

    #[test]
    fn test_invalid_edge_endpoint_handles() {
        let graph = create_graph();

        assert!(
            graph
                .edge_endpoint_handles(&50)
                .is_err_and(|e| matches!(e, GraphError::IndexError(_)))
        );
    }

    #[test]
    fn test_edge_indices() {
        let graph = create_graph();

        let edge_indices = [0, 1, 2, 3];

        for edge_index in graph.edge_indices() {
            assert!(edge_indices.contains(edge_index));
        }
    }

    fn handle_of(graph: &Graph, name: &str) -> NodeHandle {
        graph.node_index_table.get(&name.into()).unwrap()
    }

    #[test]
    fn test_edges_connecting() {
        let graph = create_graph();

        let edges_connecting =
            graph.edges_connecting([handle_of(&graph, "0")], [handle_of(&graph, "1")]);

        assert_eq!(vec![&0], edges_connecting.collect::<Vec<_>>());

        let edges_connecting =
            graph.edges_connecting([handle_of(&graph, "0")], [handle_of(&graph, "3")]);

        assert_eq!(0, edges_connecting.count());

        let mut edges_connecting: Vec<_> = graph
            .edges_connecting(
                [handle_of(&graph, "0"), handle_of(&graph, "1")],
                [handle_of(&graph, "2")],
            )
            .collect();

        edges_connecting.sort();
        assert_eq!(vec![&2, &3], edges_connecting);

        let mut edges_connecting: Vec<_> = graph
            .edges_connecting(
                [handle_of(&graph, "0"), handle_of(&graph, "1")],
                [handle_of(&graph, "2"), handle_of(&graph, "3")],
            )
            .collect();

        edges_connecting.sort();
        assert_eq!(vec![&2, &3], edges_connecting);
    }

    #[test]
    fn test_edges_connecting_undirected() {
        let graph = create_graph();

        let mut edges_connecting: Vec<_> = graph
            .edges_connecting_undirected([handle_of(&graph, "0")], [handle_of(&graph, "1")])
            .collect();

        edges_connecting.sort();
        assert_eq!(vec![&0, &1], edges_connecting);
    }

    #[test]
    fn test_contains_edge() {
        let graph = create_graph();

        assert!(graph.contains_edge(&0));

        assert!(!graph.contains_edge(&50));
    }

    #[test]
    fn test_outgoing_neighbors() {
        let graph = create_graph();

        let neighbors = graph.outgoing_neighbors(handle_of(&graph, "0")).unwrap();

        assert_eq!(2, neighbors.count());
    }

    #[test]
    fn test_invalid_outgoing_neighbors() {
        let graph = create_graph();

        assert!(graph.node_index_table.get(&"50".into()).is_none());
    }

    #[test]
    fn test_neighbors() {
        let graph = create_graph();

        let neighbors = graph.outgoing_neighbors(handle_of(&graph, "2")).unwrap();
        assert_eq!(0, neighbors.count());

        let neighbors = graph.neighbors(handle_of(&graph, "2")).unwrap();
        assert_eq!(2, neighbors.count());
    }

    #[test]
    fn test_invalid_neighbors() {
        let graph = create_graph();

        assert!(graph.node_index_table.get(&"50".into()).is_none());
    }
}
