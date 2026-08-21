use super::{equality_bare, equality_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Mask, QueryResult,
    capabilities::ValueEquality,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::EqualTo,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "EqualTo")]
#[plan(optimizer_hints(empty = if_all))]
pub struct EqualToOperation<A> {
    #[argument]
    argument: A,
}

impl<I: IndexDomain, V: ValueEquality, A: ArgumentSource<Keyed<I>, V>> ElementKernel<Indexed<I, V>>
    for EqualToOperation<A>
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(equality_indexed::<_, V, A>(
            graphrecord,
            prepared,
            V::equal,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        let selectivity = input.distinct.map(|distinct| 1.0 / distinct.max(1) as f64);

        Estimate {
            selectivity,
            ..input.with_unknown_distinct()
        }
    }
}

impl<V: ValueEquality + BareValueDomain, A: ArgumentSource<Unaligned, V>> ElementKernel<Bare<V>>
    for EqualToOperation<A>
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(equality_bare::<V, A>(
            graphrecord,
            prepared,
            V::equal,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        let selectivity = input.distinct.map(|distinct| 1.0 / distinct.max(1) as f64);

        Estimate {
            selectivity,
            ..input.with_unknown_distinct()
        }
    }
}

impl<E, A> EqualTo<A> for E
where
    EqualToOperation<A>: Operation,
    E: Build<EqualToOperation<A>>,
{
    type Output = E::Output;

    fn equal_to(&self, argument: A) -> Self::Output {
        self.build(EqualToOperation { argument })
    }
}

operation_manifest! {
    EqualToOperation<A> {
        method: EqualTo<A>::equal_to;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueEquality>;
            argument: A: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, Mask>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueEquality + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<Mask>;
            emission: ArgumentRetention;
        }
    }
}
