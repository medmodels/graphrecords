use super::{ordering_bare, ordering_indexed};
use crate::{
    Bare, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult, ValueType,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::GreaterThan,
    value::ValueOrdering,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "GreaterThan")]
#[plan(optimizer_hints(empty = if_all))]
pub struct GreaterThanOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for GreaterThanOperation<A> {
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

impl<I, V, A> ElementKernel<Indexed<I, V>> for GreaterThanOperation<A>
where
    I: IndexDomain,
    for<'a> V: ValueOrdering + ValueType<Value<'a> = <V as ValueType>::Owned>,
    V::Owned: Debug + Display + Send + Sync,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(ordering_indexed::<_, A, V>(
            prepared,
            Self::LABEL,
            V::ordering,
            Ordering::is_gt,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for GreaterThanOperation<A>
where
    for<'a> V: ValueOrdering + ValueType<Value<'a> = <V as ValueType>::Owned>,
    V::Owned: Debug + Display + Send + Sync,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(ordering_bare::<A, V>(
            prepared,
            Self::LABEL,
            V::ordering,
            Ordering::is_gt,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<O, A> GreaterThan<A> for O
where
    GreaterThanOperation<A>: Operation,
    O: Apply<GreaterThanOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn greater_than(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            GreaterThanOperation { argument },
        ))
    }
}
