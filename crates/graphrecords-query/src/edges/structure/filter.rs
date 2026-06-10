use crate::{
    EdgeOperand, EvaluateContext, EvaluateOperand, Explain, Operand,
    bool::BoolMaskOperand,
    edges::EdgeOperandContext,
    execution::ExecutionContext,
    optimizer::{Cardinality, HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Stats},
    traits::Filter,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use graphrecords_utils::aliases::GrHashMap;

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = EdgeOperand, optimizer_hints(commutes_with_filter, distinct, empty = if_any))]
#[explain(label = "Filter")]
pub struct FilterContext {
    #[input]
    input: EdgeOperand,
    #[input]
    mask: BoolMaskOperand<EdgeOperand>,
}

impl Cardinality for FilterContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        let input_cardinality = self.input.context().cardinality(stats) as f64;

        (input_cardinality * self.mask.context().selectivity(stats)).round() as usize
    }
}

impl EvaluateContext for FilterContext {
    type Operand = EdgeOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let edge_indices = self.input.evaluate(graphrecord, context)?;

        let mask_by_index: GrHashMap<_, _> = self.mask.evaluate(graphrecord, context)?.collect();

        Ok(Box::new(edge_indices.filter(move |edge_index| {
            mask_by_index.get(edge_index).copied().unwrap_or(false)
        })))
    }
}

impl EdgeOperandContext for FilterContext {}

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
