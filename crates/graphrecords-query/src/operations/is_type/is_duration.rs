use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Mask, QueryResult,
    capabilities::{PayloadKind, ValueScalarKindTest},
    element::{Pipeline, Preserving},
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::IsDuration,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "IsDuration")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct IsDurationOperation;

impl<I: IndexDomain, V: ValueScalarKindTest> ElementKernel<Indexed<I, V>> for IsDurationOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|value: QueryResult<_>| {
            value.map(|value| matches!(V::kind(&value), PayloadKind::Duration))
        }))
    }
}

impl<V: ValueScalarKindTest + BareValueDomain> ElementKernel<Bare<V>> for IsDurationOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|value: QueryResult<_>| {
            value.map(|value| matches!(V::kind(&value), PayloadKind::Duration))
        }))
    }
}

impl<E: Build<IsDurationOperation>> IsDuration for E {
    type Output = E::Output;

    fn is_duration(&self) -> Self::Output {
        self.build(IsDurationOperation)
    }
}

operation_manifest! {
    IsDurationOperation {
        method: IsDuration::is_duration;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueScalarKindTest>;
            input: Indexed<I, V>;
            output: Indexed<I, Mask>;
            emission: Preserving;
        }

        kernel {
            parameters: <V: ValueScalarKindTest + BareValueDomain>;
            input: Bare<V>;
            output: Bare<Mask>;
            emission: Preserving;
        }
    }
}
