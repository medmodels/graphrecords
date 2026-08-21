use super::{ordering_bare, ordering_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Mask, QueryResult,
    capabilities::ValueOrdering,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::LessThan,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "LessThan")]
#[plan(optimizer_hints(empty = if_all))]
pub struct LessThanOperation<A> {
    #[argument]
    argument: A,
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for LessThanOperation<A>
where
    I: IndexDomain,
    V: ValueOrdering,
    A: ArgumentSource<Keyed<I>, V>,
    V::Owned: Debug + Display + Send + Sync,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(ordering_indexed::<_, V, A>(
            graphrecord,
            prepared,
            V::ordering,
            Ordering::is_lt,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for LessThanOperation<A>
where
    V: ValueOrdering + BareValueDomain,
    A: ArgumentSource<Unaligned, V>,
    V::Owned: Debug + Display + Send + Sync,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(ordering_bare::<V, A>(
            graphrecord,
            prepared,
            V::ordering,
            Ordering::is_lt,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<E, A> LessThan<A> for E
where
    LessThanOperation<A>: Operation,
    E: Build<LessThanOperation<A>>,
{
    type Output = E::Output;

    fn less_than(&self, argument: A) -> Self::Output {
        self.build(LessThanOperation { argument })
    }
}

operation_manifest! {
    LessThanOperation<A> {
        method: LessThan<A>::less_than;
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
