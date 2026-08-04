use super::{numeric_bare, numeric_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::ValueAbsolute,
    element::Preserving,
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Absolute,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Absolute")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct AbsoluteOperation;

impl Prepare for AbsoluteOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V> ElementKernel<Indexed<I, V>> for AbsoluteOperation
where
    I: IndexDomain,
    V: ValueAbsolute,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(numeric_indexed::<I, V, _>(Self::LABEL, V::absolute))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V> ElementKernel<Bare<V>> for AbsoluteOperation
where
    V: ValueAbsolute + BareValueDomain,
{
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(numeric_bare::<V, _>(Self::LABEL, V::absolute))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O: Apply<AbsoluteOperation>> Absolute for O {
    type ReturnOperand = O::Output;

    fn abs(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), AbsoluteOperation))
    }
}

operation_manifest! {
    AbsoluteOperation {
        method: Absolute::abs;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueAbsolute>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: Preserving;
        }

        kernel {
            parameters: <V: ValueAbsolute + BareValueDomain>;
            input: Bare<V>;
            output: Bare<V>;
            emission: Preserving;
        }
    }
}
