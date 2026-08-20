use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Mask, QueryResult,
    capabilities::{PayloadKind, ValueKindTest},
    element::{Pipeline, Preserving},
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::IsString,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "IsString")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct IsStringOperation;

impl<I: IndexDomain, V: ValueKindTest> ElementKernel<Indexed<I, V>> for IsStringOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|value: QueryResult<_>| {
            value.map(|value| matches!(V::kind(&value), PayloadKind::String))
        }))
    }
}

impl<V: ValueKindTest + BareValueDomain> ElementKernel<Bare<V>> for IsStringOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|value: QueryResult<_>| {
            value.map(|value| matches!(V::kind(&value), PayloadKind::String))
        }))
    }
}

impl<E: Build<IsStringOperation>> IsString for E {
    type Output = E::Output;

    fn is_string(&self) -> Self::Output {
        self.build(IsStringOperation)
    }
}

operation_manifest! {
    IsStringOperation {
        method: IsString::is_string;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueKindTest>;
            input: Indexed<I, V>;
            output: Indexed<I, Mask>;
            emission: Preserving;
        }

        kernel {
            parameters: <V: ValueKindTest + BareValueDomain>;
            input: Bare<V>;
            output: Bare<Mask>;
            emission: Preserving;
        }
    }
}
