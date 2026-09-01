mod add_edges;
mod add_edges_in_group;
mod add_edges_to_group;
mod add_group;
mod add_nodes;
mod add_nodes_in_group;
mod add_nodes_to_group;
mod clear;
mod freeze_schema;
mod remove_edge_attributes;
mod remove_edges;
mod remove_edges_from_group;
mod remove_groups;
mod remove_node_attributes;
mod remove_nodes;
mod remove_nodes_from_group;
mod replace_edge_attributes;
mod replace_node_attributes;
mod set_edge_attributes;
mod set_node_attributes;
mod set_schema;
mod unfreeze_schema;

mod sealed {
    pub trait Sealed {}
}

pub use self::{
    add_edges::AddEdges, add_edges_in_group::AddEdgesInGroup, add_edges_to_group::AddEdgesToGroup,
    add_group::AddGroup, add_nodes::AddNodes, add_nodes_in_group::AddNodesInGroup,
    add_nodes_to_group::AddNodesToGroup, clear::Clear, freeze_schema::FreezeSchema,
    remove_edge_attributes::RemoveEdgeAttributes, remove_edges::RemoveEdges,
    remove_edges_from_group::RemoveEdgesFromGroup, remove_groups::RemoveGroups,
    remove_node_attributes::RemoveNodeAttributes, remove_nodes::RemoveNodes,
    remove_nodes_from_group::RemoveNodesFromGroup, replace_edge_attributes::ReplaceEdgeAttributes,
    replace_node_attributes::ReplaceNodeAttributes, set_edge_attributes::SetEdgeAttributes,
    set_node_attributes::SetNodeAttributes, set_schema::SetSchema, unfreeze_schema::UnfreezeSchema,
};
#[cfg(feature = "plugins")]
use crate::graphrecord::{GraphRecord, plugins::Plugin};
use crate::{errors::GraphRecordResult, graphrecord::state::GraphState};

pub trait Change: sealed::Sealed {
    fn apply(&self, state: GraphState) -> GraphRecordResult<GraphState>;

    #[cfg(feature = "plugins")]
    fn pre_dispatch(
        self: Box<Self>,
        plugin: &dyn Plugin,
        record: &GraphRecord,
    ) -> GraphRecordResult<Changes>;

    #[cfg(feature = "plugins")]
    fn post_dispatch(
        &self,
        plugin: &dyn Plugin,
        previous: &GraphRecord,
        candidate: &GraphRecord,
    ) -> GraphRecordResult<()>;
}

#[derive(Default)]
pub struct Changes(Vec<Box<dyn Change>>);

impl Changes {
    #[must_use]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn push(&mut self, change: impl Change + 'static) {
        self.0.push(Box::new(change));
    }

    pub fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &dyn Change> {
        self.0.iter().map(Box::as_ref)
    }
}

impl From<Vec<Box<dyn Change>>> for Changes {
    fn from(changes: Vec<Box<dyn Change>>) -> Self {
        Self(changes)
    }
}

impl<C: Change + 'static> From<C> for Changes {
    fn from(change: C) -> Self {
        Self(vec![Box::new(change)])
    }
}

impl IntoIterator for Changes {
    type IntoIter = std::vec::IntoIter<Box<dyn Change>>;
    type Item = Box<dyn Change>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod test {
    use super::{AddGroup, AddNodes, Change, Changes, RemoveGroups};
    use crate::graphrecord::{batch::NodeBatch, state::GraphState};

    fn create_add_nodes() -> AddNodes {
        AddNodes::new(NodeBatch::default())
    }

    #[test]
    fn test_changes_new() {
        assert!(Changes::new().is_empty());
        assert_eq!(0, Changes::new().len());
        assert!(Changes::default().is_empty());
    }

    #[test]
    fn test_changes_push() {
        let mut changes = Changes::new();

        changes.push(create_add_nodes());
        changes.push(AddGroup::new("lorem".into()));

        assert_eq!(2, changes.len());
    }

    #[test]
    fn test_changes_extend() {
        let mut changes = Changes::new();
        changes.push(create_add_nodes());

        let mut other = Changes::new();
        other.push(AddGroup::new("lorem".into()));
        other.push(AddGroup::new("ipsum".into()));

        changes.extend(other);

        assert_eq!(3, changes.len());

        changes.extend(Changes::new());

        assert_eq!(3, changes.len());
    }

    #[test]
    fn test_changes_len() {
        let mut changes = Changes::new();

        assert_eq!(0, changes.len());

        changes.push(create_add_nodes());

        assert_eq!(1, changes.len());
    }

    #[test]
    fn test_changes_is_empty() {
        let mut changes = Changes::new();

        assert!(changes.is_empty());

        changes.push(create_add_nodes());

        assert!(!changes.is_empty());
    }

    #[test]
    fn test_changes_iter() {
        let mut changes = Changes::new();

        changes.push(create_add_nodes());
        changes.push(AddGroup::new("lorem".into()));

        assert_eq!(2, changes.iter().count());
    }

    #[test]
    fn test_changes_from() {
        let from_boxed = Changes::from(vec![Box::new(create_add_nodes()) as Box<dyn Change>]);

        assert_eq!(1, from_boxed.len());

        let from_single = Changes::from(create_add_nodes());

        assert_eq!(1, from_single.len());
    }

    #[test]
    fn test_changes_into_iter() {
        let mut changes = Changes::new();

        changes.push(AddGroup::new("lorem".into()));
        changes.push(RemoveGroups::new(vec!["lorem".into()]));

        let mut state = GraphState::new();

        for change in changes {
            state = change.apply(state).unwrap();
        }

        assert_eq!(0, state.group_count());
    }
}
