use super::{
    EdgeIndex, GraphRecordAttribute,
    intern_table::{GroupHandle, NodeHandle},
};
use crate::errors::{GraphRecordError, GraphRecordResult};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub type Group = GraphRecordAttribute;

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(super) struct GroupMapping {
    pub(super) nodes_in_group: GrHashMap<GroupHandle, GrHashSet<NodeHandle>>,
    pub(super) edges_in_group: GrHashMap<GroupHandle, GrHashSet<EdgeIndex>>,
    pub(super) groups_of_node: GrHashMap<NodeHandle, GrHashSet<GroupHandle>>,
    pub(super) groups_of_edge: GrHashMap<EdgeIndex, GrHashSet<GroupHandle>>,
}

impl GroupMapping {
    pub fn add_group(
        &mut self,
        group: GroupHandle,
        node_indices: Option<Vec<NodeHandle>>,
        edge_indices: Option<Vec<EdgeIndex>>,
    ) -> GraphRecordResult<()> {
        if self.nodes_in_group.contains_key(&group) {
            return Err(GraphRecordError::AssertionError(format!(
                "Group with handle {group:?} already exists"
            )));
        }

        let node_indices = node_indices.unwrap_or_default();
        let edge_indices = edge_indices.unwrap_or_default();

        self.nodes_in_group
            .insert(group, node_indices.iter().copied().collect());
        self.edges_in_group
            .insert(group, edge_indices.iter().copied().collect());

        for node_index in node_indices {
            self.groups_of_node
                .entry(node_index)
                .or_default()
                .insert(group);
        }

        for edge_index in edge_indices {
            self.groups_of_edge
                .entry(edge_index)
                .or_default()
                .insert(group);
        }

        Ok(())
    }

    pub fn add_node_to_group(
        &mut self,
        group: GroupHandle,
        node_index: NodeHandle,
    ) -> GraphRecordResult<()> {
        let nodes_in_group = self.nodes_in_group.get_mut(&group).ok_or_else(|| {
            GraphRecordError::IndexError(format!("Cannot find group with handle {group:?}"))
        })?;

        if !nodes_in_group.insert(node_index) {
            return Err(GraphRecordError::AssertionError(format!(
                "Node with handle {node_index:?} already in group with handle {group:?}"
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
        group: GroupHandle,
        edge_index: EdgeIndex,
    ) -> GraphRecordResult<()> {
        let edges_in_group = self.edges_in_group.get_mut(&group).ok_or_else(|| {
            GraphRecordError::IndexError(format!("Cannot find group with handle {group:?}"))
        })?;

        if !edges_in_group.insert(edge_index) {
            return Err(GraphRecordError::AssertionError(format!(
                "Edge with index {edge_index} already in group with handle {group:?}"
            )));
        }

        self.groups_of_edge
            .entry(edge_index)
            .or_default()
            .insert(group);

        Ok(())
    }

    pub fn remove_group(&mut self, group: GroupHandle) -> GraphRecordResult<()> {
        let nodes_in_group = self.nodes_in_group.remove(&group).ok_or_else(|| {
            GraphRecordError::IndexError(format!("Cannot find group with handle {group:?}"))
        })?;

        for node in nodes_in_group {
            self.groups_of_node
                .get_mut(&node)
                .expect("Node must exist")
                .remove(&group);
        }

        let edges_in_group = self.edges_in_group.remove(&group).ok_or_else(|| {
            GraphRecordError::IndexError(format!("Cannot find group with handle {group:?}"))
        })?;

        for edge in edges_in_group {
            self.groups_of_edge
                .get_mut(&edge)
                .expect("Edge must exist")
                .remove(&group);
        }

        Ok(())
    }

    pub fn remove_node(&mut self, node_index: NodeHandle) {
        let Some(groups_of_node) = self.groups_of_node.remove(&node_index) else {
            return;
        };

        for group in groups_of_node {
            self.nodes_in_group
                .get_mut(&group)
                .expect("Group must exist")
                .remove(&node_index);
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn remove_edge(&mut self, edge_index: &EdgeIndex) {
        let Some(groups_of_edge) = self.groups_of_edge.remove(edge_index) else {
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
        group: GroupHandle,
        node_index: NodeHandle,
    ) -> GraphRecordResult<()> {
        let nodes_in_group = self.nodes_in_group.get_mut(&group).ok_or_else(|| {
            GraphRecordError::IndexError(format!("Cannot find group with handle {group:?}"))
        })?;

        nodes_in_group
            .remove(&node_index)
            .then_some(())
            .ok_or_else(|| {
                GraphRecordError::AssertionError(format!(
                    "Node with handle {node_index:?} not in group with handle {group:?}"
                ))
            })?;

        if let Some(groups) = self.groups_of_node.get_mut(&node_index) {
            groups.remove(&group);
        }

        Ok(())
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn remove_edge_from_group(
        &mut self,
        group: GroupHandle,
        edge_index: &EdgeIndex,
    ) -> GraphRecordResult<()> {
        let edges_in_group = self.edges_in_group.get_mut(&group).ok_or_else(|| {
            GraphRecordError::IndexError(format!("Cannot find group with handle {group:?}"))
        })?;

        edges_in_group
            .remove(edge_index)
            .then_some(())
            .ok_or_else(|| {
                GraphRecordError::AssertionError(format!(
                    "Edge with index {edge_index} not in group with handle {group:?}"
                ))
            })?;

        if let Some(groups) = self.groups_of_edge.get_mut(edge_index) {
            groups.remove(&group);
        }

        Ok(())
    }

    pub fn groups(&self) -> impl Iterator<Item = GroupHandle> + use<'_> {
        self.nodes_in_group.keys().copied()
    }

    pub fn nodes_in_group(
        &self,
        group: GroupHandle,
    ) -> GraphRecordResult<impl Iterator<Item = NodeHandle> + use<'_>> {
        Ok(self
            .nodes_in_group
            .get(&group)
            .ok_or_else(|| {
                GraphRecordError::IndexError(format!("Cannot find group with handle {group:?}"))
            })?
            .iter()
            .copied())
    }

    pub fn edges_in_group(
        &self,
        group: GroupHandle,
    ) -> GraphRecordResult<impl Iterator<Item = &EdgeIndex> + use<'_>> {
        Ok(self
            .edges_in_group
            .get(&group)
            .ok_or_else(|| {
                GraphRecordError::IndexError(format!("Cannot find group with handle {group:?}"))
            })?
            .iter())
    }

    pub fn groups_of_node(
        &self,
        node_index: NodeHandle,
    ) -> impl Iterator<Item = GroupHandle> + use<'_> {
        self.groups_of_node
            .get(&node_index)
            .into_iter()
            .flatten()
            .copied()
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn groups_of_edge(
        &self,
        edge_index: &EdgeIndex,
    ) -> impl Iterator<Item = GroupHandle> + use<'_> {
        self.groups_of_edge
            .get(edge_index)
            .into_iter()
            .flatten()
            .copied()
    }

    pub fn group_count(&self) -> usize {
        self.nodes_in_group.len()
    }

    pub fn contains_group(&self, group: GroupHandle) -> bool {
        self.nodes_in_group.contains_key(&group)
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
    use super::super::intern_table::{GroupHandle, NodeHandle};
    use super::GroupMapping;
    use crate::errors::GraphRecordError;

    #[cfg(not(feature = "safe-handles"))]
    fn group(index: u32) -> GroupHandle {
        GroupHandle::new(index)
    }

    #[cfg(feature = "safe-handles")]
    fn group(index: u32) -> GroupHandle {
        GroupHandle::new(0, index)
    }

    #[cfg(not(feature = "safe-handles"))]
    fn node(index: u32) -> NodeHandle {
        NodeHandle::new(index)
    }

    #[cfg(feature = "safe-handles")]
    fn node(index: u32) -> NodeHandle {
        NodeHandle::new(0, index)
    }

    #[test]
    fn test_add_group() {
        let mut group_mapping = GroupMapping::default();

        assert_eq!(0, group_mapping.group_count());

        group_mapping.add_group(group(0), None, None).unwrap();

        assert_eq!(1, group_mapping.group_count());

        group_mapping
            .add_group(group(1), Some(vec![node(0), node(1)]), Some(vec![0, 1]))
            .unwrap();

        assert_eq!(2, group_mapping.group_count());
        assert_eq!(2, group_mapping.nodes_in_group(group(1)).unwrap().count());
        assert_eq!(2, group_mapping.edges_in_group(group(1)).unwrap().count());
    }

    #[test]
    fn test_invalid_add_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group(group(0), None, None).unwrap();

        assert!(
            group_mapping
                .add_group(group(0), None, None)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_add_node_to_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group(group(0), None, None).unwrap();

        assert_eq!(0, group_mapping.nodes_in_group(group(0)).unwrap().count());

        group_mapping.add_node_to_group(group(0), node(0)).unwrap();

        assert_eq!(1, group_mapping.nodes_in_group(group(0)).unwrap().count());
    }

    #[test]
    fn test_invalid_add_node_to_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), Some(vec![node(0)]), None)
            .unwrap();

        assert!(
            group_mapping
                .add_node_to_group(group(0), node(0))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_add_edge_to_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group(group(0), None, None).unwrap();

        assert_eq!(0, group_mapping.edges_in_group(group(0)).unwrap().count());

        group_mapping.add_edge_to_group(group(0), 0).unwrap();

        assert_eq!(1, group_mapping.edges_in_group(group(0)).unwrap().count());
    }

    #[test]
    fn test_invalid_add_edge_to_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), None, Some(vec![0]))
            .unwrap();

        assert!(
            group_mapping
                .add_edge_to_group(group(0), 0)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group(group(0), None, None).unwrap();

        assert_eq!(1, group_mapping.group_count());

        group_mapping.remove_group(group(0)).unwrap();

        assert_eq!(0, group_mapping.group_count());
    }

    #[test]
    fn test_invalid_remove_group() {
        let mut group_mapping = GroupMapping::default();

        assert!(
            group_mapping
                .remove_group(group(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_remove_node() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), Some(vec![node(0)]), None)
            .unwrap();

        assert_eq!(1, group_mapping.nodes_in_group(group(0)).unwrap().count());

        group_mapping.remove_node(node(0));

        assert_eq!(0, group_mapping.nodes_in_group(group(0)).unwrap().count());
    }

    #[test]
    fn test_remove_edge() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), None, Some(vec![0]))
            .unwrap();

        assert_eq!(1, group_mapping.edges_in_group(group(0)).unwrap().count());

        group_mapping.remove_edge(&0);

        assert_eq!(0, group_mapping.edges_in_group(group(0)).unwrap().count());
    }

    #[test]
    fn test_remove_node_from_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), Some(vec![node(0), node(1)]), None)
            .unwrap();

        assert_eq!(2, group_mapping.nodes_in_group(group(0)).unwrap().count());

        group_mapping
            .remove_node_from_group(group(0), node(0))
            .unwrap();

        assert_eq!(1, group_mapping.nodes_in_group(group(0)).unwrap().count());
    }

    #[test]
    fn test_invalid_remove_node_from_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), Some(vec![node(0)]), None)
            .unwrap();

        assert!(
            group_mapping
                .remove_node_from_group(group(50), node(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            group_mapping
                .remove_node_from_group(group(0), node(50))
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_remove_edge_from_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), None, Some(vec![0, 1]))
            .unwrap();

        assert_eq!(2, group_mapping.edges_in_group(group(0)).unwrap().count());

        group_mapping.remove_edge_from_group(group(0), &0).unwrap();

        assert_eq!(1, group_mapping.edges_in_group(group(0)).unwrap().count());
    }

    #[test]
    fn test_invalid_remove_edge_from_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), None, Some(vec![0]))
            .unwrap();

        assert!(
            group_mapping
                .remove_edge_from_group(group(50), &0)
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );

        assert!(
            group_mapping
                .remove_edge_from_group(group(0), &50)
                .is_err_and(|e| matches!(e, GraphRecordError::AssertionError(_)))
        );
    }

    #[test]
    fn test_groups() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group(group(0), None, None).unwrap();

        assert_eq!(1, group_mapping.groups().count());
    }

    #[test]
    fn test_nodes_in_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), Some(vec![node(0), node(1)]), None)
            .unwrap();

        assert_eq!(2, group_mapping.nodes_in_group(group(0)).unwrap().count());
    }

    #[test]
    fn test_invalid_nodes_in_group() {
        let group_mapping = GroupMapping::default();

        assert!(
            group_mapping
                .nodes_in_group(group(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_edges_in_group() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), None, Some(vec![0, 1]))
            .unwrap();

        assert_eq!(2, group_mapping.edges_in_group(group(0)).unwrap().count());
    }

    #[test]
    fn test_invalid_edges_in_group() {
        let group_mapping = GroupMapping::default();

        assert!(
            group_mapping
                .edges_in_group(group(0))
                .is_err_and(|e| matches!(e, GraphRecordError::IndexError(_)))
        );
    }

    #[test]
    fn test_groups_of_node() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), Some(vec![node(0)]), None)
            .unwrap();

        assert_eq!(1, group_mapping.groups_of_node(node(0)).count());
    }

    #[test]
    fn test_groups_of_edge() {
        let mut group_mapping = GroupMapping::default();

        group_mapping
            .add_group(group(0), None, Some(vec![0]))
            .unwrap();

        assert_eq!(1, group_mapping.groups_of_edge(&0).count());
    }

    #[test]
    fn test_group_count() {
        let mut group_mapping = GroupMapping::default();

        assert_eq!(0, group_mapping.group_count());

        group_mapping.add_group(group(0), None, None).unwrap();

        assert_eq!(1, group_mapping.group_count());
    }

    #[test]
    fn test_contains_group() {
        let mut group_mapping = GroupMapping::default();

        assert!(!group_mapping.contains_group(group(0)));

        group_mapping.add_group(group(0), None, None).unwrap();

        assert!(group_mapping.contains_group(group(0)));
    }

    #[test]
    fn test_clear() {
        let mut group_mapping = GroupMapping::default();

        group_mapping.add_group(group(0), None, None).unwrap();

        assert_eq!(1, group_mapping.group_count());

        group_mapping.clear();

        assert_eq!(0, group_mapping.group_count());
    }
}
