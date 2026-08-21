use super::{DefiniteElementExpression, ElementExpression, ElementsExpression, Expression};
use crate::{
    EvaluateContext, EvaluateExpression, Explain, QueryResult, Unordered,
    execution::EvaluationCache,
    optimizer::{
        Count, CountKind, Estimate, Estimated, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, StateView, graphrecord::NodeIndex};

pub type NodesExpression<O> = ElementsExpression<NodeIndex, O>;
pub type NodeExpression = ElementExpression<NodeIndex>;
pub type DefiniteNodeExpression = DefiniteElementExpression<NodeIndex>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(expression = NodesExpression<Unordered>)]
pub struct AllNodes;

#[must_use]
pub fn nodes() -> NodesExpression<Unordered> {
    NodesExpression::new(AllNodes)
}

impl Estimated for AllNodes {
    fn estimate(&self, stats: &Stats) -> Estimate {
        let nodes = stats.get::<Count>(&CountKind::Nodes);

        Estimate::values(nodes, nodes)
    }
}

impl EvaluateContext for AllNodes {
    type Expression = NodesExpression<Unordered>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<<Self::Expression as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(
            StateView::of(graphrecord)
                .node_addresses()
                .map(|address| (address, Ok(()))),
        ))
    }
}
