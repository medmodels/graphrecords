use super::{numeric_bare, numeric_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::ValueSign,
    element::Preserving,
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Sign,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Sign")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct SignOperation;

impl Prepare for SignOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V> ElementKernel<Indexed<I, V>> for SignOperation
where
    I: IndexDomain,
    V: ValueSign,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(numeric_indexed::<I, V, _>(Self::LABEL, V::sign))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V> ElementKernel<Bare<V>> for SignOperation
where
    V: ValueSign + BareValueDomain,
{
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(numeric_bare::<V, _>(Self::LABEL, V::sign))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O: Apply<SignOperation>> Sign for O {
    type ReturnOperand = O::Output;

    fn sign(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SignOperation))
    }
}

operation_manifest! {
    SignOperation {
        method: Sign::sign;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueSign>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: Preserving;
        }

        kernel {
            parameters: <V: ValueSign + BareValueDomain>;
            input: Bare<V>;
            output: Bare<V>;
            emission: Preserving;
        }
    }
}
