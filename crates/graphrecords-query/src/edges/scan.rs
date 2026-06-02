use crate::{
    BoxedIterator, EdgeOperand,
    edges::EdgeOperandContext,
    execution::ExecutionContext,
    optimizer::{Cardinality, Count, CountKind, PlanNode, Stats},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};

#[derive(PlanNode)]
#[plan_node(crate = "crate", operand = "EdgeOperand", distinct)]
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
