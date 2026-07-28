mod context;
pub mod error;
pub mod execution;
pub mod explain;
pub mod operands;
pub mod operations;
pub mod optimizer;
pub mod prelude;
pub mod selection;
mod traits;

use crate::{
    optimizer::Optimizer,
    selection::{ReturnOperand, Selection},
};
pub use context::{EvaluateContext, OperandContext};
pub use error::{
    Diagnostic, DuplicateIndex, ErrorGroup, External, Failure, FailureKind, IncomparableValues,
    IncomparableValuesAt, QueryResult,
};
pub use explain::{Explain, Explanation, Labeled};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex},
};
pub use operands::{
    Arity, AttributeName, Bare, BucketOwned, CheckedIndexedLaneBuilder, Definite,
    DefiniteEdgeOperand, DefiniteNodeOperand, DefiniteReferenceOperand,
    DuplicateExpandedChildIndex, EdgeOperand, EdgesOperand, ElementShape, EntityReference,
    EvaluateOperand, ExpandedChild, ExpandedIndex, ExpandedIndexOwned, ExpandedIndexReference,
    FailureKindValue, FailureValue, GroupOperand, IndexValue, Indexed, KeyFailureOwned, Mask,
    Multiple, NoChildIndex, NodeOperand, NodesOperand, Operand, OrderState, Ordered,
    PartitionBucketParts, PartitionKeyFailureParts, PartitionOwned, PartitionOwnedParts,
    PartitionParts, PreparedIndexedMultiple, ReferenceOperand, ReferencesOperand, Return,
    ReturnShape, ReturnValueType, Scalar, Single, Unit, Unordered, ValueType,
};
use std::{
    any::Any,
    fmt::{Debug, Display},
    hash::Hash,
};
pub use traits::*;

pub type BoxedIterator<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub type Position = usize;

pub trait OwnedIndex: Any + Debug + Display + Send + Sync {}

impl<T: Any + Debug + Display + Send + Sync> OwnedIndex for T {}

pub trait IndexDomain: 'static + Clone {
    type Owned: 'static + Clone + Eq + Hash + OwnedIndex;

    type Index<'a>: Clone + Eq + Hash
    where
        Self: 'a;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned;

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_>;
}

pub trait EntityDomain: IndexDomain {
    fn resolve_index<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Owned,
    ) -> GraphRecordResult<Self::Index<'a>>;
}

#[derive(Clone, Debug)]
pub struct Positional;

impl IndexDomain for Positional {
    type Index<'a> = Position;
    type Owned = Position;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }
}

impl IndexDomain for EdgeIndex {
    type Index<'a> = &'a Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        **index
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        owned
    }
}

impl IndexDomain for NodeIndex {
    type Index<'a> = &'a Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        (*index).clone()
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        owned
    }
}

impl IndexDomain for FailureKind {
    type Index<'a> = Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }
}

impl IndexDomain for GraphRecordValue {
    type Index<'a> = Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        index.clone()
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        owned.clone()
    }
}

impl IndexDomain for AttributeName {
    type Index<'a> = GraphRecordAttribute;
    type Owned = GraphRecordAttribute;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        index.clone()
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        owned.clone()
    }
}

impl IndexDomain for bool {
    type Index<'a> = Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }
}

impl EntityDomain for NodeIndex {
    fn resolve_index<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Owned,
    ) -> GraphRecordResult<Self::Index<'a>> {
        graphrecord.resolve_node_index(index)
    }
}

impl EntityDomain for EdgeIndex {
    fn resolve_index<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Owned,
    ) -> GraphRecordResult<Self::Index<'a>> {
        graphrecord.resolve_edge_index(index)
    }
}

mod sealed {
    pub trait Sealed {}
}

pub trait QueryNodes {
    fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;

    fn query_nodes_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;
}

impl QueryNodes for GraphRecord {
    fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_node(self, query)
    }

    fn query_nodes_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_node_with(self, optimizer, query)
    }
}

pub trait QueryEdges {
    fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;

    fn query_edges_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;
}

impl QueryEdges for GraphRecord {
    fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_edge(self, query)
    }

    fn query_edges_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgesOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_edge_with(self, optimizer, query)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Attribute, Filter, InGroup, QueryNodes};
    use graphrecords_core::GraphRecord;
    use std::collections::HashMap;

    #[test]
    fn test_query_nodes() {
        let mut graphrecord = GraphRecord::from_tuples(
            vec![
                (
                    "0".into(),
                    HashMap::from([("lorem".into(), "ipsum".into())]),
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
            ],
            None,
            None,
        )
        .unwrap();

        graphrecord
            .add_node_to_group("lorem".into(), "0".into())
            .unwrap();
        graphrecord
            .add_node_to_group("lorem".into(), "1".into())
            .unwrap();
        graphrecord
            .add_node_to_group("ipsum".into(), "0".into())
            .unwrap();

        let selection = QueryNodes::query_nodes(&graphrecord, |node| {
            let nodes = node.filter(node.in_group("lorem".into()) & !node.in_group("ipsum".into()));

            nodes.attribute("amet".into())
        });

        println!("{}", selection.explain());

        println!("{:?}", selection.evaluate().unwrap().collect::<Vec<_>>());
    }
}
