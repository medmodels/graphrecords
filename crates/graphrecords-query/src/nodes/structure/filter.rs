use crate::{
    BoxedIterator, Evaluate, Explain, Operand,
    bool::BoolMaskOperand,
    execution::ExecutionContext,
    nodes::{NodeOperand, NodeOperandContext},
    optimizer::{Cardinality, HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Stats},
    traits::Filter,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};
use graphrecords_utils::aliases::GrHashMap;

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = NodeOperand, optimizer_hints(commutes_with_filter, distinct, empty = if_any))]
#[explain(label = "Filter")]
pub struct FilterContext {
    #[input]
    input: NodeOperand,
    #[input]
    mask: BoolMaskOperand<NodeOperand>,
}

impl Cardinality for FilterContext {
    fn cardinality(&self, stats: &Stats) -> usize {
        let input_cardinality = self.input.context().cardinality(stats) as f64;

        (input_cardinality * self.mask.context().selectivity(stats)).round() as usize
    }
}

impl NodeOperandContext for FilterContext {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>> {
        let node_indices = self.input.evaluate(graphrecord, context)?;

        let mask_by_index: GrHashMap<_, _> = self.mask.evaluate(graphrecord, context)?.collect();

        Ok(Box::new(node_indices.filter(move |node_index| {
            mask_by_index.get(node_index).copied().unwrap_or(false)
        })))
    }
}

impl Filter for NodeOperand {
    type MaskOperand = BoolMaskOperand<Self>;
    type ReturnOperand = Self;

    fn filter(&self, mask: Self::MaskOperand) -> Self::ReturnOperand {
        Self::new(FilterContext {
            input: self.clone(),
            mask,
        })
    }
}
