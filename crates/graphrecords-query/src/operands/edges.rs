use super::OperandHandle;
use crate::{
    EvaluateContext, EvaluateOperand, Explain, Indexed, Multiple, Operand, QueryResult, Unit,
    execution::EvaluationCache,
    optimizer::{
        Cardinality, Cost, Count, CountKind, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, graphrecord::EdgeIndex};

pub type EdgeOperand = OperandHandle<Indexed<EdgeIndex, Unit>, Multiple>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(operand = EdgeOperand, optimizer_hints(distinct))]
pub struct AllEdges;

impl Cost<EdgeOperand> for AllEdges {
    fn cost(&self, stats: &Stats) -> <EdgeOperand as Operand>::Cost {
        Cardinality(stats.get::<Count>(&CountKind::Edges))
    }
}

impl EvaluateContext for AllEdges {
    type Operand = EdgeOperand;

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
