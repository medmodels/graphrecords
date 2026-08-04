use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Mask, Operand, QueryResult,
    capabilities::{PayloadKind, ValueKindTest},
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::IsInt,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "IsInt")]
#[plan(optimizer_hints(
    commutes_with_filter,
    allows_limit_pushdown,
    empty = if_any
))]
pub struct IsIntOperation;

impl Prepare for IsIntOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueKindTest> ElementKernel<Indexed<I, V>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|value: QueryResult<_>| {
            value.map(|value| matches!(V::kind(&value), PayloadKind::Int))
        }))
    }
}

impl<V: ValueKindTest + BareValueDomain> ElementKernel<Bare<V>> for IsIntOperation {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|value: QueryResult<_>| {
            value.map(|value| matches!(V::kind(&value), PayloadKind::Int))
        }))
    }
}

impl<O: Apply<IsIntOperation>> IsInt for O {
    type ReturnOperand = O::Output;

    fn is_int(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), IsIntOperation))
    }
}

operation_manifest! {
    IsIntOperation {
        method: IsInt::is_int;
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
