use super::{ordering_bare, ordering_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult,
    capabilities::ValueOrdering,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::LessThanOrEqualTo,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "LessThanOrEqualTo")]
#[plan(optimizer_hints(empty = if_all))]
pub struct LessThanOrEqualToOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for LessThanOrEqualToOperation<A> {
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

impl<I, V, A> ElementKernel<Indexed<I, V>> for LessThanOrEqualToOperation<A>
where
    I: IndexDomain,
    V: ValueOrdering,
    A: ArgumentSource<Keyed<I>, V>,
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
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for LessThanOrEqualToOperation<A>
where
    V: ValueOrdering + BareValueDomain,
    A: ArgumentSource<Unaligned, V>,
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
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<O, A> LessThanOrEqualTo<A> for O
where
    LessThanOrEqualToOperation<A>: Operation,
    O: Apply<LessThanOrEqualToOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn less_than_or_equal_to(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            LessThanOrEqualToOperation { argument },
        ))
    }
}

operation_manifest! {
    LessThanOrEqualToOperation<A> {
        method: LessThanOrEqualTo<A>::less_than_or_equal_to;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueOrdering>;
            argument: A: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, Mask>;
            emission: ArgumentRetention;
            where V::Owned: Debug + Display + Send + Sync;
        }

        kernel {
            parameters: <V: ValueOrdering + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<Mask>;
            emission: ArgumentRetention;
            where V::Owned: Debug + Display + Send + Sync;
        }
    }
}
