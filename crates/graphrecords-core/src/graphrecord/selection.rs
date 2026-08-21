use crate::{
    errors::{GraphRecordError, GraphRecordResult},
    graphrecord::{EdgeIndex, GraphRecord, GroupIndex, Identifier, NodeIndex},
};

pub trait EntityDomain: Sized {
    fn contains(graphrecord: &GraphRecord, index: &Self) -> bool;

    fn not_found(indices: Vec<Self>) -> GraphRecordError;

    fn verify(graphrecord: &GraphRecord, indices: Vec<Self>) -> GraphRecordResult<Vec<Self>> {
        let (present, absent): (Vec<_>, Vec<_>) = indices
            .into_iter()
            .partition(|index| Self::contains(graphrecord, index));

        if !absent.is_empty() {
            return Err(Self::not_found(absent));
        }

        Ok(present)
    }
}

impl EntityDomain for NodeIndex {
    fn contains(graphrecord: &GraphRecord, index: &Self) -> bool {
        graphrecord.contains_node(index)
    }

    fn not_found(indices: Vec<Self>) -> GraphRecordError {
        GraphRecordError::NodesNotFound {
            node_indices: indices,
        }
    }
}

impl EntityDomain for EdgeIndex {
    fn contains(graphrecord: &GraphRecord, index: &Self) -> bool {
        graphrecord.contains_edge(index)
    }

    fn not_found(indices: Vec<Self>) -> GraphRecordError {
        GraphRecordError::EdgesNotFound {
            edge_indices: indices,
        }
    }
}

impl EntityDomain for GroupIndex {
    fn contains(graphrecord: &GraphRecord, index: &Self) -> bool {
        graphrecord.contains_group(index)
    }

    fn not_found(indices: Vec<Self>) -> GraphRecordError {
        GraphRecordError::GroupsNotFound {
            group_indices: indices,
        }
    }
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a selection of this entity domain",
    note = "a selection is an index, a list of indices, an expression, or a series of the target domain"
)]
pub trait MultipleSelection<E: EntityDomain> {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>>;
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a single selection of this entity domain",
    note = "a single selection is an index, or an expression or series of the target domain that selects at most one element"
)]
pub trait SingleSelection<E: EntityDomain>: MultipleSelection<E> {}

impl MultipleSelection<Self> for NodeIndex {
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<Self>> {
        Ok(vec![self])
    }
}

impl SingleSelection<Self> for NodeIndex {}

impl MultipleSelection<NodeIndex> for Identifier {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<NodeIndex>> {
        NodeIndex::from(self).resolve(graphrecord)
    }
}

impl SingleSelection<NodeIndex> for Identifier {}

impl MultipleSelection<NodeIndex> for &str {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<NodeIndex>> {
        NodeIndex::from(self).resolve(graphrecord)
    }
}

impl SingleSelection<NodeIndex> for &str {}

impl MultipleSelection<NodeIndex> for String {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<NodeIndex>> {
        NodeIndex::from(self).resolve(graphrecord)
    }
}

impl SingleSelection<NodeIndex> for String {}

impl MultipleSelection<NodeIndex> for i64 {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<NodeIndex>> {
        NodeIndex::from(self).resolve(graphrecord)
    }
}

impl SingleSelection<NodeIndex> for i64 {}

impl MultipleSelection<Self> for EdgeIndex {
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<Self>> {
        Ok(vec![self])
    }
}

impl SingleSelection<Self> for EdgeIndex {}

impl MultipleSelection<Self> for GroupIndex {
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<Self>> {
        Ok(vec![self])
    }
}

impl SingleSelection<Self> for GroupIndex {}

impl MultipleSelection<GroupIndex> for Identifier {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<GroupIndex>> {
        GroupIndex::from(self).resolve(graphrecord)
    }
}

impl SingleSelection<GroupIndex> for Identifier {}

impl MultipleSelection<GroupIndex> for &str {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<GroupIndex>> {
        GroupIndex::from(self).resolve(graphrecord)
    }
}

impl SingleSelection<GroupIndex> for &str {}

impl MultipleSelection<GroupIndex> for String {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<GroupIndex>> {
        GroupIndex::from(self).resolve(graphrecord)
    }
}

impl SingleSelection<GroupIndex> for String {}

impl MultipleSelection<GroupIndex> for i64 {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<GroupIndex>> {
        GroupIndex::from(self).resolve(graphrecord)
    }
}

impl SingleSelection<GroupIndex> for i64 {}

impl<E: EntityDomain, I: Into<E>> MultipleSelection<E> for Vec<I> {
    fn resolve(self, _graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        Ok(self.into_iter().map(Into::into).collect())
    }
}

impl<E: EntityDomain, I: Into<E>, const N: usize> MultipleSelection<E> for [I; N] {
    fn resolve(self, graphrecord: &GraphRecord) -> GraphRecordResult<Vec<E>> {
        MultipleSelection::<E>::resolve(Vec::from(self), graphrecord)
    }
}

#[cfg(test)]
mod test {
    use super::{EntityDomain, MultipleSelection};
    use crate::{
        errors::GraphRecordError,
        graphrecord::{AttributeMap, EdgeIndex, GraphRecord, GroupIndex, NodeIndex},
    };

    fn create_graphrecord() -> (GraphRecord, EdgeIndex) {
        let graphrecord = GraphRecord::new()
            .add_node("lorem", AttributeMap::new())
            .unwrap()
            .add_node("ipsum", AttributeMap::new())
            .unwrap()
            .add_edge("lorem", "ipsum", AttributeMap::new())
            .unwrap()
            .add_group("dolor")
            .unwrap();
        let edge_index = graphrecord.edge_indices().next().unwrap();

        (graphrecord, edge_index)
    }

    #[test]
    fn test_contains() {
        let (graphrecord, edge_index) = create_graphrecord();

        assert!(NodeIndex::contains(&graphrecord, &"lorem".into()));
        assert!(!NodeIndex::contains(&graphrecord, &"sit".into()));
        assert!(EdgeIndex::contains(&graphrecord, &edge_index));
        assert!(!EdgeIndex::contains(
            &graphrecord,
            &EdgeIndex::new(edge_index.tag().wrapping_add(1), edge_index.offset())
        ));
        assert!(GroupIndex::contains(&graphrecord, &"dolor".into()));
        assert!(!GroupIndex::contains(&graphrecord, &"sit".into()));
    }

    #[test]
    fn test_not_found() {
        assert!(matches!(
            NodeIndex::not_found(vec!["lorem".into()]),
            GraphRecordError::NodesNotFound { node_indices }
                if node_indices == vec![NodeIndex::from("lorem")]
        ));
        assert!(matches!(
            EdgeIndex::not_found(vec![EdgeIndex::new(0, 0)]),
            GraphRecordError::EdgesNotFound { edge_indices }
                if edge_indices == vec![EdgeIndex::new(0, 0)]
        ));
        assert!(matches!(
            GroupIndex::not_found(vec!["lorem".into()]),
            GraphRecordError::GroupsNotFound { group_indices }
                if group_indices == vec![GroupIndex::from("lorem")]
        ));
    }

    #[test]
    fn test_verify() {
        let (graphrecord, edge_index) = create_graphrecord();

        let verified =
            NodeIndex::verify(&graphrecord, vec!["lorem".into(), "ipsum".into()]).unwrap();

        assert_eq!(
            vec![NodeIndex::from("lorem"), NodeIndex::from("ipsum")],
            verified
        );

        let verified = EdgeIndex::verify(&graphrecord, vec![edge_index]).unwrap();

        assert_eq!(vec![edge_index], verified);

        let verified = GroupIndex::verify(&graphrecord, Vec::new()).unwrap();

        assert_eq!(Vec::<GroupIndex>::new(), verified);
    }

    #[test]
    fn test_invalid_verify() {
        let (graphrecord, edge_index) = create_graphrecord();

        let result = NodeIndex::verify(
            &graphrecord,
            vec!["sit".into(), "lorem".into(), "amet".into()],
        );

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::NodesNotFound { node_indices }
                if node_indices == vec![NodeIndex::from("sit"), NodeIndex::from("amet")]
        )));

        let missing_edge_index =
            EdgeIndex::new(edge_index.tag().wrapping_add(1), edge_index.offset());

        let result = EdgeIndex::verify(&graphrecord, vec![missing_edge_index]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::EdgesNotFound { edge_indices }
                if edge_indices == vec![missing_edge_index]
        )));

        let result = GroupIndex::verify(&graphrecord, vec!["sit".into()]);

        assert!(result.is_err_and(|error| matches!(
            error,
            GraphRecordError::GroupsNotFound { group_indices }
                if group_indices == vec![GroupIndex::from("sit")]
        )));
    }

    #[test]
    fn test_resolve() {
        let (graphrecord, edge_index) = create_graphrecord();

        let selected = MultipleSelection::<NodeIndex>::resolve("lorem", &graphrecord).unwrap();

        assert_eq!(vec![NodeIndex::from("lorem")], selected);

        let selected =
            MultipleSelection::<NodeIndex>::resolve(vec!["lorem", "sit"], &graphrecord).unwrap();

        assert_eq!(
            vec![NodeIndex::from("lorem"), NodeIndex::from("sit")],
            selected
        );

        let selected =
            MultipleSelection::<NodeIndex>::resolve(Vec::<NodeIndex>::new(), &graphrecord).unwrap();

        assert_eq!(Vec::<NodeIndex>::new(), selected);

        let selected = MultipleSelection::<EdgeIndex>::resolve([edge_index], &graphrecord).unwrap();

        assert_eq!(vec![edge_index], selected);

        let selected = MultipleSelection::<GroupIndex>::resolve("dolor", &graphrecord).unwrap();

        assert_eq!(vec![GroupIndex::from("dolor")], selected);
    }
}
