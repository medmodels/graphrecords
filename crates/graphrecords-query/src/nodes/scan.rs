use crate::{
    EvaluateContext, Explain, NodeOperand,
    execution::ExecutionContext,
    nodes::NodeOperandContext,
    optimizer::{
        Cardinality, Count, CountKind, HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Stats,
    },
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = NodeOperand, optimizer_hints(distinct))]
pub struct AllNodes;

impl Cardinality for AllNodes {
    fn cardinality(&self, stats: &Stats) -> usize {
        stats.get::<Count>(&CountKind::Nodes)
    }
}

impl EvaluateContext for AllNodes {
    type Operand = NodeOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        _context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as crate::EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(graphrecord.node_indices()))
    }
}

impl NodeOperandContext for AllNodes {}
