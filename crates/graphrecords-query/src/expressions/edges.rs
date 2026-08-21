use super::{DefiniteElementExpression, ElementExpression, ElementsExpression, Expression};
use crate::{
    EvaluateContext, EvaluateExpression, Explain, QueryResult, Unordered,
    execution::EvaluationCache,
    optimizer::{
        Count, CountKind, Estimate, Estimated, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, StateView, graphrecord::EdgeIndex};

pub type EdgesExpression<O> = ElementsExpression<EdgeIndex, O>;
pub type EdgeExpression = ElementExpression<EdgeIndex>;
pub type DefiniteEdgeExpression = DefiniteElementExpression<EdgeIndex>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(expression = EdgesExpression<Unordered>)]
pub struct AllEdges;

#[must_use]
pub fn edges() -> EdgesExpression<Unordered> {
    EdgesExpression::new(AllEdges)
}

impl Estimated for AllEdges {
    fn estimate(&self, stats: &Stats) -> Estimate {
        let edges = stats.get::<Count>(&CountKind::Edges);

        Estimate::values(edges, edges)
    }
}

impl EvaluateContext for AllEdges {
    type Expression = EdgesExpression<Unordered>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<<Self::Expression as EvaluateExpression>::ReturnValue<'a>> {
        Ok(Box::new(
            StateView::of(graphrecord)
                .edge_addresses()
                .map(|address| (address, Ok(()))),
        ))
    }
}
