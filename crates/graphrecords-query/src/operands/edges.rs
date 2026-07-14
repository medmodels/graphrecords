use super::OperandHandle;
use crate::{
    EvaluateContext, EvaluateOperand, Explain, Indexed, Multiple, QueryResult, Unit, Unordered,
    execution::EvaluationCache,
    optimizer::{
        Count, CountKind, Estimate, Estimated, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, graphrecord::EdgeIndex};

pub type EdgeOperand<O> = OperandHandle<Indexed<EdgeIndex, Unit>, Multiple<O>>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(operand = EdgeOperand<Unordered>, optimizer_hints(distinct))]
pub struct AllEdges;

impl Estimated for AllEdges {
    fn estimate(&self, stats: &Stats) -> Estimate {
        let edges = stats.get::<Count>(&CountKind::Edges);

        Estimate::values(edges, edges)
    }
}

impl EvaluateContext for AllEdges {
    type Operand = EdgeOperand<Unordered>;

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
