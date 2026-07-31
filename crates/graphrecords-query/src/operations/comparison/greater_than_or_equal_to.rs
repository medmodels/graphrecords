use super::{ordering_bare, ordering_indexed};
use crate::{
    Bare, BareValueType, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult,
    capabilities::ValueOrdering,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::GreaterThanOrEqualTo,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "GreaterThanOrEqualTo")]
#[plan(optimizer_hints(empty = if_all))]
pub struct GreaterThanOrEqualToOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for GreaterThanOrEqualToOperation<A> {
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

impl<I, V, A> ElementKernel<Indexed<I, V>> for GreaterThanOrEqualToOperation<A>
where
    I: IndexDomain,
    V: ValueOrdering,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
    V::Owned: Debug + Display + Send + Sync,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(ordering_indexed::<_, V, A>(
            prepared,
            Self::LABEL,
            V::ordering,
            Ordering::is_ge,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for GreaterThanOrEqualToOperation<A>
where
    V: ValueOrdering + BareValueType,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
    V::Owned: Debug + Display + Send + Sync,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(ordering_bare::<V, A>(
            prepared,
            Self::LABEL,
            V::ordering,
            Ordering::is_ge,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<O, A> GreaterThanOrEqualTo<A> for O
where
    GreaterThanOrEqualToOperation<A>: Operation,
    O: Apply<GreaterThanOrEqualToOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn greater_than_or_equal_to(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            GreaterThanOrEqualToOperation { argument },
        ))
    }
}
