use super::{arithmetic_bare, arithmetic_indexed};
use crate::{
    Bare, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Subtract,
    value::ValueSubtract,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Subtract")]
#[plan(optimizer_hints(empty = if_all))]
pub struct SubtractOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for SubtractOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.argument.prepare(graphrecord, cache)
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for SubtractOperation<A>
where
    I: IndexDomain,
    V: ValueSubtract,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(arithmetic_indexed::<_, V, A>(
            prepared,
            Self::LABEL,
            V::subtract,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, A> ElementKernel<Bare<V>> for SubtractOperation<A>
where
    V: ValueSubtract,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    type Emission = A::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(arithmetic_bare::<V, A>(prepared, Self::LABEL, V::subtract))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O, A> Subtract<A> for O
where
    SubtractOperation<A>: Operation,
    O: Apply<SubtractOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn subtract(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            SubtractOperation { argument },
        ))
    }
}
