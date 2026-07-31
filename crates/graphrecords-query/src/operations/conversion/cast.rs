use crate::{
    Bare, BareValueType, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::ValueCast,
    cast::CastTarget,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Cast,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Cast")]
#[plan(optimizer_hints(empty = if_any))]
pub struct CastOperation<T: CastTarget> {
    #[explain(label)]
    target: T,
}

impl<T: CastTarget> Prepare for CastOperation<T> {
    type Prepared<'a>
        = &'a T
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(&self.target)
    }
}

impl<I, V, T> ElementKernel<Indexed<I, V>> for CastOperation<T>
where
    I: IndexDomain,
    V: ValueCast<T>,
    T: CastTarget,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::keyed(move |index, outcome: QueryResult<_>| {
            outcome.and_then(|value| {
                V::cast(Self::LABEL, value, prepared).map_err(|failure| failure.at::<I>(&index))
            })
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, T> ElementKernel<Bare<V>> for CastOperation<T>
where
    V: ValueCast<T> + BareValueType,
    T: CastTarget,
{
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            outcome.and_then(|value| V::cast(Self::LABEL, value, prepared))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O, T> Cast<T> for O
where
    CastOperation<T>: Operation,
    O: Apply<CastOperation<T>>,
    T: CastTarget,
{
    type ReturnOperand = O::Output;

    fn cast(&self, target: T) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            CastOperation { target },
        ))
    }
}
