use super::{EdgeIndex, GraphRecordAttribute, NodeIndex};
use crate::errors::{GraphRecordError, GraphRecordResult};
use graphrecords_utils::aliases::{GrHashMap, GrHashMapEntry, GrHashSet};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub type Group = GraphRecordAttribute;

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(super) struct GroupMapping {
    pub(super) nodes_in_group: GrHashMap<Group, GrHashSet<NodeIndex>>,
    pub(super) edges_in_group: GrHashMap<Group, GrHashSet<EdgeIndex>>,
    pub(super) groups_of_node: GrHashMap<NodeIndex, GrHashSet<Group>>,
    pub(super) groups_of_edge: GrHashMap<EdgeIndex, GrHashSet<Group>>,
}

impl GroupMapping {
    #[allow(clippy::needless_pass_by_value)]
    pub fn add_group(
        &mut self,
        group: Group,
        node_indices: Option<Vec<NodeIndex>>,
        edge_indices: Option<Vec<EdgeIndex>>,
    ) -> GraphRecordResult<()> {
        if self.nodes_in_group.contains_key(&group) {
            return Err(GraphRecordError::AssertionError(format!(
                "Group {group} already exists"
            )));
        }

        let node_indices = node_indices.unwrap_or_default();
        let edge_indices = edge_indices.unwrap_or_default();

        self.nodes_in_group
            .insert(group.clone(), node_indices.iter().cloned().collect());
        self.edges_in_group
            .insert(group.clone(), edge_indices.iter().copied().collect());

        for node_index in node_indices {
            self.groups_of_node
                .entry(node_index)
                .or_default()
                .insert(group.clone());
        }

        for edge_index in edge_indices {
            self.groups_of_edge
                .entry(edge_index)
                .or_default()
                .insert(group.clone());
        }

        Ok(())
    }

    pub fn add_node_to_group(
        &mut self,
        group: Group,
        node_index: NodeIndex,
    ) -> GraphRecordResult<()> {
        // TODO: This was changed. Add a test for adding to a non-existing group
        let nodes_in_group = self.nodes_in_group.entry(group.clone());

        if let GrHashMapEntry::Vacant(_) = nodes_in_group {
            self.edges_in_group
                .insert(group.clone(), GrHashSet::default());
        }

        let nodes_in_group = nodes_in_group.or_default();

        if !nodes_in_group.insert(node_index.clone()) {
            return Err(GraphRecordError::AssertionError(format!(
                "Node with index {node_index} already in group {group}"
            )));
        }

        self.groups_of_node
            .entry(node_index)
            .or_default()
            .insert(group);

        Ok(())
    }

    pub fn add_edge_to_group(
        &mut self,
        group: Group,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        // TODO: This was changed. Add a test for adding to a non-existing group
        let edges_in_group = self.edges_in_group.entry(group.clone());

        if let GrHashMapEntry::Vacant(_) = edges_in_group {
            self.nodes_in_group
                .insert(group.clone(), GrHashSet::default());
        }

        let edges_in_group = edges_in_group.or_default();

        if !edges_in_group.insert(edge_index) {
            return Err(GraphRecordError::AssertionError(format!(
                "Edge with index {edge_index} already in group {group}"
            )));
        }

        self.groups_of_edge
            .entry(edge_index)
            .or_default()
            .insert(group);

        Ok(())
    }

    pub fn remove_group(&mut self, group: &Group) -> GraphRecordResult<()> {
        let nodes_in_group = self
            .nodes_in_group
            .remove(group)
            .ok_or_else(|| GraphRecordError::IndexError(format!("Cannot find group {group}")))?;

        for node in nodes_in_group {
            self.groups_of_node
                .get_mut(&node)
                .expect("Node must exist")
                .remove(group);
        }

        let edges_in_group = self
            .edges_in_group
            .remove(group)
            .ok_or_else(|| GraphRecordError::IndexError(format!("Cannot find group {group}")))?;

        for edge in edges_in_group {
            self.groups_of_edge
                .get_mut(&edge)
                .expect("Edge must exist")
                .remove(group);
        }

        Ok(())
    }

    pub fn remove_node(&mut self, node_index: &NodeIndex) {
        let groups_of_node = self.groups_of_node.remove(node_index);

        let Some(groups_of_node) = groups_of_node else {
            return;
        };

        for group in groups_of_node {
            self.nodes_in_group
                .get_mut(&group)
                .expect("Group must exist")
                .remove(node_index);
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn remove_edge(&mut self, edge_index: &EdgeIndex) {
        let groups_of_edge = self.groups_of_edge.remove(edge_index);

        let Some(groups_of_edge) = groups_of_edge else {
            return;
        };

        for group in groups_of_edge {
            self.edges_in_group
                .get_mut(&group)
                .expect("Group must exist")
                .remove(edge_index);
        }
    }

    pub fn remove_node_from_group(
        &mut self,
        group: &Group,
        node_index: &NodeIndex,
    ) -> GraphRecordResult<()> {
        let nodes_in_group = self
            .nodes_in_group
            .get_mut(group)
            .ok_or_else(|| GraphRecordError::IndexError(format!("Cannot find group {group}")))?;

        nodes_in_group
            .remove(node_index)
            .then_some(())
            .ok_or_else(|| {
                GraphRecordError::AssertionError(format!(
                    "Node with index {node_index} not in group {group}"
                ))
            })
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn remove_edge_from_group(
        &mut self,
        group: &Group,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        let edges_in_group = self
            .edges_in_group
            .get_mut(group)
            .ok_or_else(|| GraphRecordError::IndexError(format!("Cannot find group {group}")))?;

        edges_in_group
            .remove(edge_index)
            .then_some(())
            .ok_or_else(|| {
                GraphRecordError::AssertionError(format!(
                    "Edge with index {edge_index} not in group {group}"
                ))
            })
    }

    pub fn groups(&self) -> impl Iterator<Item = &Group> {
        self.nodes_in_group.keys()
    }

    pub fn nodes_in_group(
        &self,
        group: &Group,
    ) -> GraphRecordResult<impl Iterator<Item = &NodeIndex> + use<'_>> {
        Ok(self
            .nodes_in_group
            .get(group)
            .ok_or_else(|| GraphRecordError::IndexError(format!("Cannot find group {group}")))?
            .iter())
    }

    pub fn edges_in_group(
        &self,
        group: &Group,
    ) -> GraphRecordResult<impl Iterator<Item = &EdgeIndex> + use<'_>> {
        Ok(self
            .edges_in_group
            .get(group)
            .ok_or_else(|| GraphRecordError::IndexError(format!("Cannot find group {group}")))?
            .iter())
    }

    pub fn groups_of_node(&self, node_index: &NodeIndex) -> impl Iterator<Item = &Group> + use<'_> {
        self.groups_of_node.get(node_index).into_iter().flatten()
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn groups_of_edge(&self, edge_index: &EdgeIndex) -> impl Iterator<Item = &Group> + use<'_> {
        self.groups_of_edge.get(edge_index).into_iter().flatten()
    }

    pub fn group_count(&self) -> usize {
        self.nodes_in_group.len()
    }

    pub fn contains_group(&self, group: &Group) -> bool {
        self.nodes_in_group.contains_key(group)
    }

    pub fn clear(&mut self) {
        self.nodes_in_group.clear();
        self.edges_in_group.clear();
        self.groups_of_node.clear();
        self.groups_of_edge.clear();
    }
}

#[cfg(test)]
mod test {
    use super::GroupMapping;
    use crate::errors::GraphRecordError;

    #[test]
    fn test_add_group() {
        let mut group_mapping = GroupMapping::default();

        assert_eq!(0, group_mapping.group_count());

        group_mapping.add_group("0".into(), None, None).unwrap();

        assert_eq!(1, group_mapping.group_count());

        group_mapping
            .add_group(
                "1".into(),
                Some(vec!["0".into(), "1".into()]),
                Some(vec![0, 1]),
            )
            .unwrap();

        assert_eq!(2, group_mapping.group_count());
        assert_eq!(
            2,
            group_mapping.nodes_in_group(&"1".into()).unwrap().count()
        );
        assert_eq!(
            2,
            group_mapping.edges_in_group(&"1".into()).unwrap().count()
        );
    }

    #[test]
    fn test_invalid_add_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group("0".into(), None, None).unwrap();

        // Adding an already existing group should fail
        assert!(
            group_mapping
                .add_group("0".into(), None, None)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_add_node_to_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group("0".into(), None, None).unwrap();

        assert_eq!(
            0,
            group_mapping.nodes_in_group(&"0".into()).unwrap().count()
        );

        group_mapping
            .add_node_to_group("0".into(), "0".into())
            .unwrap();

        assert_eq!(
            1,
            group_mapping.nodes_in_group(&"0".into()).unwrap().count()
        );
    }

    #[test]
    fn test_invalid_add_node_to_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), Some(vec!["0".into()]), None)
            .unwrap();

        // Adding a node to a group that already is in the group should fail
        assert!(
            group_mapping
                .add_node_to_group("0".into(), "0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_add_edge_to_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group("0".into(), None, None).unwrap();

        assert_eq!(
            0,
            group_mapping.edges_in_group(&"0".into()).unwrap().count()
        );

        group_mapping.add_edge_to_group("0".into(), 0).unwrap();

        assert_eq!(
            1,
            group_mapping.edges_in_group(&"0".into()).unwrap().count()
        );
    }

    #[test]
    fn test_invalid_add_edge_to_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), None, Some(vec![0]))
            .unwrap();

        // Adding an edge to a group that already is in the group should fail
        assert!(
            group_mapping
                .add_edge_to_group("0".into(), 0)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group("0".into(), None, None).unwrap();

        assert_eq!(1, group_mapping.group_count());

        group_mapping.remove_group(&"0".into()).unwrap();

        assert_eq!(0, group_mapping.group_count());
    }

    #[test]
    fn test_invalid_remove_group() {
        let mut group_mapping = GroupMapping::default();

        // Removing a non-existing group should fail
        assert!(
            group_mapping
                .remove_group(&"0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_remove_node() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), Some(vec!["0".into()]), None)
            .unwrap();

        assert_eq!(
            1,
            group_mapping.nodes_in_group(&"0".into()).unwrap().count()
        );

        group_mapping.remove_node(&"0".into());

        assert_eq!(
            0,
            group_mapping.nodes_in_group(&"0".into()).unwrap().count()
        );
    }

    #[test]
    fn test_remove_edge() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), None, Some(vec![0]))
            .unwrap();

        assert_eq!(
            1,
            group_mapping.edges_in_group(&"0".into()).unwrap().count()
        );

        group_mapping.remove_edge(&0);

        assert_eq!(
            0,
            group_mapping.edges_in_group(&"0".into()).unwrap().count()
        );
    }

    #[test]
    fn test_remove_node_from_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), Some(vec!["0".into(), "1".into()]), None)
            .unwrap();

        assert_eq!(
            2,
            group_mapping.nodes_in_group(&"0".into()).unwrap().count()
        );

        group_mapping
            .remove_node_from_group(&"0".into(), &"0".into())
            .unwrap();

        assert_eq!(
            1,
            group_mapping.nodes_in_group(&"0".into()).unwrap().count()
        );
    }

    #[test]
    fn test_invalid_remove_node_from_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), Some(vec!["0".into()]), None)
            .unwrap();

        // Removing a node from a non-existing group should fail
        assert!(
            group_mapping
                .remove_node_from_group(&"50".into(), &"0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing a non-existing node from a group should fail
        assert!(
            group_mapping
                .remove_node_from_group(&"0".into(), &"50".into())
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_edge_from_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), None, Some(vec![0, 1]))
            .unwrap();

        assert_eq!(
            2,
            group_mapping.edges_in_group(&"0".into()).unwrap().count()
        );

        group_mapping
            .remove_edge_from_group(&"0".into(), &0)
            .unwrap();

        assert_eq!(
            1,
            group_mapping.edges_in_group(&"0".into()).unwrap().count()
        );
    }

    #[test]
    fn test_invalid_remove_edge_from_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), None, Some(vec![0]))
            .unwrap();

        // Removing an edge from a non-existing group should fail
        assert!(
            group_mapping
                .remove_edge_from_group(&"50".into(), &0)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        // Removing a non-existing edge from a group should fail
        assert!(
            group_mapping
                .remove_edge_from_group(&"0".into(), &50)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_groups() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group("0".into(), None, None).unwrap();

        assert_eq!(1, group_mapping.groups().count());
    }

    #[test]
    fn test_nodes_in_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), Some(vec!["0".into(), "1".into()]), None)
            .unwrap();

        assert_eq!(
            2,
            group_mapping.nodes_in_group(&"0".into()).unwrap().count()
        );
    }

    #[test]
    fn test_invalid_nodes_in_group() {
        let group_mapping = GroupMapping::default();

        // Querying the nodes in a non-existing group should fail
        assert!(
            group_mapping
                .nodes_in_group(&"0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_edges_in_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), None, Some(vec![0, 1]))
            .unwrap();

        assert_eq!(
            2,
            group_mapping.edges_in_group(&"0".into()).unwrap().count()
        );
    }

    #[test]
    fn test_invalid_edges_in_group() {
        let group_mapping = GroupMapping::default();

        // Querying the edges in a non-existing group should fail
        assert!(
            group_mapping
                .edges_in_group(&"0".into())
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_groups_of_node() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), Some(vec!["0".into()]), None)
            .unwrap();

        assert_eq!(1, group_mapping.groups_of_node(&"0".into()).count());
    }

    #[test]
    fn test_groups_of_edge() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group("0".into(), None, Some(vec![0]))
            .unwrap();

        assert_eq!(1, group_mapping.groups_of_edge(&0).count());
    }

    #[test]
    fn test_group_count() {
        let mut group_mapping = GroupMapping::default();

        assert_eq!(0, group_mapping.group_count());

        group_mapping.add_group("0".into(), None, None).unwrap();

        assert_eq!(1, group_mapping.group_count());
    }

    #[test]
    fn test_contains_group() {
        let mut group_mapping = GroupMapping::default();

        assert!(!group_mapping.contains_group(&"0".into()));

        group_mapping.add_group("0".into(), None, None).unwrap();

        assert!(group_mapping.contains_group(&"0".into()));
    }

    #[test]
    fn test_clear() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group("0".into(), None, None).unwrap();

        assert_eq!(1, group_mapping.group_count());

        group_mapping.clear();

        assert_eq!(0, group_mapping.group_count());
    }
}
