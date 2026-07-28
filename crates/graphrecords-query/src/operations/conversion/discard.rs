use crate::{
    Explain, IndexDomain, Indexed, Operand, QueryResult, Unit, ValueType,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::Discard,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Discard")]
#[plan(optimizer_hints(empty = if_any))]
pub struct DiscardOperation;

impl Prepare for DiscardOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueType> ElementKernel<Indexed<I, V>> for DiscardOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Unit>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|outcome: QueryResult<_>| {
            outcome.map(|_| ())
        }))
    }
}

impl<O: Apply<DiscardOperation>> Discard for O {
    type ReturnOperand = O::Output;

    fn discard(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), DiscardOperation))
    }
}
