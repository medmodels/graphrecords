pub mod bool;
pub mod edges;
pub mod group_by;
pub mod nodes;
mod operand_traits;
pub mod selection;
pub mod values;

use graphrecords_core::GraphRecord;
pub use nodes::NodeOperand;
pub use operand_traits::*;

use crate::{
    edges::EdgeOperand,
    selection::{ReturnOperand, Selection},
};

pub type BoxedIterator<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub trait RootOperand {
    type Index<'a>;
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
    use crate::{Attribute, InGroup, QueryNodes, Where};
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
            let mask = node.in_group("lorem".into());

            let nodes = node.r#where(mask);

            nodes.attribute("lorem".into())
        });

        println!("{:?}", selection.evaluate().unwrap().collect::<Vec<_>>());
    }
}
