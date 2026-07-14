use super::OperandHandle;
use crate::{
    EvaluateContext, EvaluateOperand, Explain, Indexed, Multiple, QueryResult, Unit, Unordered,
    execution::EvaluationCache,
    optimizer::{
        Count, CountKind, Estimate, Estimated, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};

pub type NodeOperand<O> = OperandHandle<Indexed<NodeIndex, Unit>, Multiple<O>>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(operand = NodeOperand<Unordered>, optimizer_hints(distinct))]
pub struct AllNodes;

impl Estimated for AllNodes {
    fn estimate(&self, stats: &Stats) -> Estimate {
        let nodes = stats.get::<Count>(&CountKind::Nodes);

        Estimate::values(nodes, nodes)
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
