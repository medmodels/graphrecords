use crate::{
    BoxedIterator, EdgeOperand, Operand, RootOperand,
    bool::BoolMaskOperand,
    edges::EdgeOperandContext,
    execution::ExecutionContext,
    optimizer::{Cardinality, Explain, OptimizerHints, PlanNode, Stats},
    traits::Filter,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};
use graphrecords_utils::aliases::GrHashMap;

#[derive(PlanNode, OptimizerHints, Explain)]
#[plan_node(crate = "crate", operand = EdgeOperand)]
#[optimizer_hints(crate = "crate", commutes_with_filter, distinct, empty = if_any)]
#[explain(crate = "crate", label = "Filter")]
pub struct FilterContext {
    #[plan_node(input)]
    input: EdgeOperand,
    #[plan_node(input)]
    mask: BoolMaskOperand<EdgeOperand>,
}

impl Cardinality for FilterContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        let input_cardinality = self.input.context().cardinality(stats) as f64;

        (input_cardinality * self.mask.context().selectivity(stats)).round() as usize
    }
}

impl EdgeOperandContext for FilterContext {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        let edge_indices = self.input.evaluate(graphrecord, context)?;

        let mask_by_index: GrHashMap<_, _> = self.mask.evaluate(graphrecord, context)?.collect();

        Ok(Box::new(edge_indices.filter(move |edge_index| {
            mask_by_index.get(edge_index).copied().unwrap_or(false)
        })))
    }
}

impl Filter for EdgeOperand {
    type MaskOperand = BoolMaskOperand<Self>;
    type ReturnOperand = Self;

    fn filter(&self, mask: Self::MaskOperand) -> Self::ReturnOperand {
        Self::new(FilterContext {
            input: self.clone(),
            mask,
        })
    }
}
