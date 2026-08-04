use super::{DefiniteElementOperand, ElementOperand, ElementsOperand};
use crate::{
    EvaluateContext, EvaluateOperand, Explain, QueryResult, Unordered,
    execution::EvaluationCache,
    optimizer::{
        Count, CountKind, Estimate, Estimated, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, graphrecord::EdgeIndex};

pub type EdgesOperand<O> = ElementsOperand<EdgeIndex, O>;
pub type EdgeOperand = ElementOperand<EdgeIndex>;
pub type DefiniteEdgeOperand = DefiniteElementOperand<EdgeIndex>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(operand = EdgesOperand<Unordered>)]
pub struct AllEdges;

impl Estimated for AllEdges {
    fn estimate(&self, stats: &Stats) -> Estimate {
        let edges = stats.get::<Count>(&CountKind::Edges);

        Estimate::values(edges, edges)
    }
}

impl EvaluateContext for AllEdges {
    type Operand = EdgesOperand<Unordered>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(
            graphrecord.edge_indices().map(|index| (index, Ok(()))),
        ))
    }
}
