use super::{DefiniteElementOperand, ElementOperand, ElementsOperand};
use crate::{
    EvaluateContext, EvaluateOperand, Explain, QueryResult, Unordered,
    execution::EvaluationCache,
    optimizer::{
        Count, CountKind, Estimate, Estimated, MatchInputs, OptimizePlan, OptimizerHints, PlanNode,
        Stats,
    },
};
use graphrecords_core::{GraphRecord, graphrecord::NodeIndex};

pub type NodesOperand<O> = ElementsOperand<NodeIndex, O>;
pub type NodeOperand = ElementOperand<NodeIndex>;
pub type DefiniteNodeOperand = DefiniteElementOperand<NodeIndex>;

#[derive(PlanNode, MatchInputs, OptimizePlan, OptimizerHints, Explain)]
#[plan(operand = NodesOperand<Unordered>)]
pub struct AllNodes;

impl Estimated for AllNodes {
    fn estimate(&self, stats: &Stats) -> Estimate {
        let nodes = stats.get::<Count>(&CountKind::Nodes);

        Estimate::values(nodes, nodes)
    }
}

impl EvaluateContext for AllNodes {
    type Operand = NodesOperand<Unordered>;

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
