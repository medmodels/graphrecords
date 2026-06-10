use crate::{
    BoxedIterator, Explain, NodeOperand,
    execution::ExecutionContext,
    nodes::NodeOperandContext,
    optimizer::{
        Cardinality, Count, CountKind, HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Stats,
    },
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = NodeOperand, optimizer_hints(distinct))]
pub struct AllNodes;

impl Cardinality for AllNodes {
    fn cardinality(&self, stats: &Stats) -> usize {
        stats.get::<Count>(&CountKind::Nodes)
    }
}

impl NodeOperandContext for AllNodes {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        _context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>> {
        Ok(Box::new(graphrecord.node_indices()))
    }
}
