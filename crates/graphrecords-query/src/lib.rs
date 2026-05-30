pub mod bool;
pub mod edges;
pub mod group;
pub mod nodes;
pub mod prelude;
pub mod selection;
mod traits;
pub mod values;

use crate::selection::{ReturnOperand, Selection};
pub use edges::EdgeOperand;
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
pub use nodes::NodeOperand;
use std::hash::Hash;
pub use traits::*;

pub type BoxedIterator<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub trait RootOperand: Send + Sync {
    type Index<'a>: Eq + Hash
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, Self::Index<'a>>>;
}

pub trait QueryNodes {
    fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodeOperand) -> R,
        R: ReturnOperand<'a>;
}

impl QueryNodes for GraphRecord {
    fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodeOperand) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_node(self, query)
    }
}

pub trait QueryEdges {
    fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgeOperand) -> R,
        R: ReturnOperand<'a>;
}

impl QueryEdges for GraphRecord {
    fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgeOperand) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_edge(self, query)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Attribute, InGroup, Not, QueryNodes, Where};
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

        let selection = QueryNodes::query_nodes(&graphrecord, |node| {
            let mask = node.in_group("lorem".into()).not();

            let nodes = node.r#where(mask);

            nodes.attribute("amet".into())
        });

        println!("{:?}", selection.evaluate().unwrap().collect::<Vec<_>>());
    }
}
