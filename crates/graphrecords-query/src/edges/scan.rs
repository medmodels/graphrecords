use crate::{
    BoxedIterator, EdgeOperand, Explain,
    edges::EdgeOperandContext,
    execution::ExecutionContext,
    optimizer::{
        Cardinality, Count, CountKind, HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Stats,
    },
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = EdgeOperand, optimizer_hints(distinct))]
pub struct AllEdges;

impl Cardinality for AllEdges {
    fn cardinality(&self, stats: &Stats) -> usize {
        stats.get::<Count>(&CountKind::Edges)
    }
}

impl EdgeOperandContext for AllEdges {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        _context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        Ok(Box::new(graphrecord.edge_indices()))
    }
}
