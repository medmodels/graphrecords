use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    ValueDomain,
    capabilities::ValueTransition,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Transition,
};
use graphrecords_core::GraphRecord;
use std::{
    any::type_name,
    fmt::{self, Write},
    marker::PhantomData,
};

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct TransitionOperation<T: ValueDomain> {
    marker: PhantomData<fn() -> T>,
}

impl<T: ValueDomain> TransitionOperation<T> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T: ValueDomain> Clone for TransitionOperation<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<T: ValueDomain> Labeled for TransitionOperation<T> {
    const LABEL: &'static str = "Transition";
}

impl<T: ValueDomain> Explain for TransitionOperation<T> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "Transition target={}", type_name::<T>())
    }
}

impl<T: ValueDomain> Prepare for TransitionOperation<T> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, S, T> ElementKernel<Indexed<I, S>> for TransitionOperation<T>
where
    I: IndexDomain,
    S: ValueTransition<T>,
    T: ValueDomain,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, T>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, S>, Self>> {
        Ok(Pipeline::keyed(|index, outcome: QueryResult<_>| {
            outcome.and_then(|value| {
                S::transition(Self::LABEL, value).map_err(|failure| failure.at::<I>(&index))
            })
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<S, T> ElementKernel<Bare<S>> for TransitionOperation<T>
where
    S: ValueTransition<T> + BareValueDomain,
    T: BareValueDomain,
{
    type Emission = Preserving;
    type OutShape = Bare<T>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<S>, Self>> {
        Ok(Pipeline::new(|outcome: QueryResult<_>| {
            outcome.and_then(|value| S::transition(Self::LABEL, value))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O: Operand> Transition for O {
    type ReturnOperand<T>
        = O::Output
    where
        T: ValueDomain,
        O: Apply<TransitionOperation<T>>;

    fn transition<T>(&self) -> Self::ReturnOperand<T>
    where
        T: ValueDomain,
        Self: Apply<TransitionOperation<T>>,
    {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            TransitionOperation::new(),
        ))
    }
}
