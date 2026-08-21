use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Mask, QueryResult,
    capabilities::{PayloadKind, ValueScalarKindTest},
    element::{Pipeline, Preserving},
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::IsBool,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "IsBool")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct IsBoolOperation;

impl<I: IndexDomain, V: ValueScalarKindTest> ElementKernel<Indexed<I, V>> for IsBoolOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|value: QueryResult<_>| {
            value.map(|value| matches!(V::kind(&value), PayloadKind::Bool))
        }))
    }
}

impl<V: ValueScalarKindTest + BareValueDomain> ElementKernel<Bare<V>> for IsBoolOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|value: QueryResult<_>| {
            value.map(|value| matches!(V::kind(&value), PayloadKind::Bool))
        }))
    }
}

impl<E: Build<IsBoolOperation>> IsBool for E {
    type Output = E::Output;

    fn is_bool(&self) -> Self::Output {
        self.build(IsBoolOperation)
    }
}

operation_manifest! {
    IsBoolOperation {
        method: IsBool::is_bool;
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
