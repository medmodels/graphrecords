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
pub use error::{Failure, IncomparableValues, QueryResult};
pub use explain::{Explain, Explanation, Labeled};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, NodeIndex},
};
pub use operands::{
    Arity, AttributeName, AttributeSet, Bare, Definite, EdgeOperand, ElementShape, EvaluateOperand,
    IndexValue, Indexed, Mask, MaskMap, Multiple, NodeOperand, Operand, OrderState, Ordered,
    Return, Scalar, Single, Unit, Unordered, ValueType,
};
use std::{fmt::Display, hash::Hash};
pub use traits::*;

pub type BoxedIterator<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub trait IndexDomain: 'static + Clone {
    type Index<'a>: Clone + Display + Eq + Hash + PartialOrd
    where
        Self: 'a;
}

#[derive(Clone)]
pub struct Positional;

impl IndexDomain for Positional {
    type Index<'a> = usize;
}

impl IndexDomain for EdgeIndex {
    type Index<'a> = &'a Self;
}

impl IndexDomain for NodeIndex {
    type Index<'a> = &'a Self;
}

mod sealed {
    pub trait Sealed {}
}

pub trait QueryNodes {
    fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodeOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;

    fn query_nodes_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodeOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;
}

impl QueryNodes for GraphRecord {
    fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodeOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_node(self, query)
    }

    fn query_nodes_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodeOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_node_with(self, optimizer, query)
    }
}

pub trait QueryEdges {
    fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgeOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;

    fn query_edges_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgeOperand<Unordered>) -> R,
        R: ReturnOperand<'a>;
}

impl QueryEdges for GraphRecord {
    fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgeOperand<Unordered>) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_edge(self, query)
    }

    fn query_edges_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgeOperand<Unordered>) -> R,
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
