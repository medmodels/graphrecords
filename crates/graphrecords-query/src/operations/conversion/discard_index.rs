use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, QueryResult,
    element::{Pipeline, Preserving},
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::DiscardIndex,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "DiscardIndex")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct DiscardIndexOperation;

impl<I: IndexDomain, V: BareValueDomain> ElementKernel<Indexed<I, V>> for DiscardIndexOperation {
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|value| value))
    }
}

impl<E: Build<DiscardIndexOperation>> DiscardIndex for E {
    type Output = E::Output;

    fn discard_index(&self) -> Self::Output {
        self.build(DiscardIndexOperation)
    }
}

operation_manifest! {
    DiscardIndexOperation {
        method: DiscardIndex::discard_index;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: BareValueDomain>;
            input: Indexed<I, V>;
            output: Bare<V>;
            emission: Preserving;
        }
    }
}
