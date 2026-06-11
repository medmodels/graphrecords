use super::OperandHandle;
use crate::{
    EvaluateContext, EvaluateOperand, Explain, Indexed, Multiple, Operand, QueryResult, Unit,
    execution::EvaluationCache,
    optimizer::{
        Cardinality, Cost, Count, CountKind, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};

pub type NodeOperand = OperandHandle<Indexed<NodeIndex, Unit>, Multiple>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(operand = NodeOperand, optimizer_hints(distinct))]
pub struct AllNodes;

impl Cost<NodeOperand> for AllNodes {
    fn cost(&self, stats: &Stats) -> <NodeOperand as Operand>::Cost {
        Cardinality(stats.get::<Count>(&CountKind::Nodes))
    }
}

impl EvaluateContext for AllNodes {
    type Operand = NodeOperand;

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
