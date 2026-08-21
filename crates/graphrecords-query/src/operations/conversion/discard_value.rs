use crate::{
    Explain, IndexDomain, Indexed, QueryResult, Unit, ValueDomain,
    element::{Pipeline, Preserving},
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::DiscardValue,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "DiscardValue")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct DiscardValueOperation;

impl<I: IndexDomain, V: ValueDomain> ElementKernel<Indexed<I, V>> for DiscardValueOperation {
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

impl<E: Build<DiscardValueOperation>> DiscardValue for E {
    type Output = E::Output;

    fn discard_value(&self) -> Self::Output {
        self.build(DiscardValueOperation)
    }
}

operation_manifest! {
    DiscardValueOperation {
        method: DiscardValue::discard_value;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: Indexed<I, V>;
            output: Indexed<I, Unit>;
            emission: Preserving;
        }
    }
}
