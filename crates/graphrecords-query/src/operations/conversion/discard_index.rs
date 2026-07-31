use crate::{
    Bare, BareValueType, Explain, IndexDomain, Indexed, Operand, QueryResult,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::DiscardIndex,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "DiscardIndex")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct DiscardIndexOperation;

impl Prepare for DiscardIndexOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: BareValueType> ElementKernel<Indexed<I, V>> for DiscardIndexOperation {
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|value| value))
    }
}

impl<O: Apply<DiscardIndexOperation>> DiscardIndex for O {
    type ReturnOperand = O::Output;

    fn discard_index(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), DiscardIndexOperation))
    }
}
