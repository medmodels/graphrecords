pub mod attributes;
pub mod bool;
pub mod edges;
pub mod execution;
pub mod explain;
pub mod group;
pub mod indices;
pub mod nodes;
pub mod optimizer;
pub mod prelude;
pub mod selection;
mod traits;
pub mod values;

use crate::{
    execution::ExecutionContext,
    group::{Discriminator, GroupedIterator},
    optimizer::{OptimizeInputs, Optimizer, PlanNode},
    selection::{ReturnOperand, Selection},
};
pub use edges::EdgeOperand;
pub use explain::{Explain, Explanation};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
pub use graphrecords_macros::Operand;
pub use nodes::NodeOperand;
use std::{hash::Hash, sync::Arc};
pub use traits::*;

pub type BoxedIterator<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub trait RootOperand:
    'static
    + Operand
    + for<'a> EvaluateOperand<ReturnValue<'a> = BoxedIterator<'a, <Self as RootOperand>::Index<'a>>>
{
    type Index<'a>: Eq + Hash
    where
        Self: 'a;
}

pub trait EvaluateContext {
    type Operand: Operand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>>;
}

pub trait EvaluateContextGrouped: EvaluateContext {
    fn evaluate_grouped<'a, D: Discriminator>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        GroupedIterator<
            'a,
            <D as Discriminator>::Key<'a>,
            <Self::Operand as EvaluateOperand>::ReturnValue<'a>,
        >,
    >;
}

pub trait EvaluateOperand {
    type ReturnValue<'a>
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>>;
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an operand",
    note = "implement `Operand` for `{Self}`, or derive it with `#[derive(Operand)]` and mark the context field `#[operand(context)]`"
)]
pub trait Operand: 'static + EvaluateOperand {
    type Context: PlanNode
        + OptimizeInputs<Output = Self>
        + Explain
        + EvaluateContext<Operand = Self>
        + ?Sized;

    fn context(&self) -> &Self::Context;

    fn as_plan_node(&self) -> &dyn PlanNode;

    fn from_context(context: Arc<Self::Context>) -> Self;

    fn downcast<T: PlanNode>(&self) -> Option<&T> {
        self.as_plan_node().downcast::<T>()
    }

    fn explain(&self) -> Explanation<'_>
    where
        Self: Sized,
    {
        Explanation::new(self)
    }
}

pub trait QueryNodes {
    fn query_nodes<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodeOperand) -> R,
        R: ReturnOperand<'a>;

    fn query_nodes_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
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

    fn query_nodes_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&NodeOperand) -> R,
        R: ReturnOperand<'a>,
    {
        Selection::new_node_with(self, optimizer, query)
    }
}

pub trait QueryEdges {
    fn query_edges<'a, Q, R>(&'a self, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgeOperand) -> R,
        R: ReturnOperand<'a>;

    fn query_edges_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
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

    fn query_edges_with<'a, Q, R>(&'a self, optimizer: &Optimizer, query: Q) -> Selection<'a, R>
    where
        Q: FnOnce(&EdgeOperand) -> R,
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
