use super::OperandHandle;
use crate::{
    EvaluateContext, EvaluateOperand, Explain, Indexed, Multiple, Operand, QueryResult, Unit,
    Unordered,
    execution::EvaluationCache,
    optimizer::{
        Cardinality, Cost, Count, CountKind, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};

pub type NodeOperand<O> = OperandHandle<Indexed<NodeIndex, Unit>, Multiple<O>>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(operand = NodeOperand<Unordered>, optimizer_hints(distinct))]
pub struct AllNodes;

impl Cost<NodeOperand<Unordered>> for AllNodes {
    fn cost(&self, stats: &Stats) -> <NodeOperand<Unordered> as Operand>::Cost {
        Cardinality(stats.get::<Count>(&CountKind::Nodes))
    }
}

impl EvaluateContext for AllNodes {
    type Operand = NodeOperand<Unordered>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(
            graphrecord.node_indices().map(|index| (index, Ok(()))),
        ))
    }
}
