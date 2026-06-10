use crate::{
    EdgeOperand, EvaluateContext, Explain,
    edges::EdgeOperandContext,
    execution::ExecutionContext,
    optimizer::{
        Cardinality, Count, CountKind, HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Stats,
    },
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = EdgeOperand, optimizer_hints(distinct))]
pub struct AllEdges;

impl Cardinality for AllEdges {
    fn cardinality(&self, stats: &Stats) -> usize {
        stats.get::<Count>(&CountKind::Edges)
    }
}

impl EvaluateContext for AllEdges {
    type Operand = EdgeOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        _context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as crate::EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(graphrecord.edge_indices()))
    }
}

impl EdgeOperandContext for AllEdges {}
