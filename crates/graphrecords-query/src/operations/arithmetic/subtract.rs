use super::{arithmetic_bare, arithmetic_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueSubtract,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::Subtract,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Subtract")]
#[plan(optimizer_hints(empty = if_all))]
pub struct SubtractOperation<A> {
    #[argument]
    argument: A,
}

impl<I: IndexDomain, V: ValueSubtract, A: ArgumentSource<Keyed<I>, V>> ElementKernel<Indexed<I, V>>
    for SubtractOperation<A>
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(arithmetic_indexed::<_, V, A>(
            graphrecord,
            prepared,
            V::subtract,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V: ValueSubtract + BareValueDomain, A: ArgumentSource<Unaligned, V>> ElementKernel<Bare<V>>
    for SubtractOperation<A>
{
    type Emission = A::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(arithmetic_bare::<V, A>(
            graphrecord,
            prepared,
            V::subtract,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E, A> Subtract<A> for E
where
    SubtractOperation<A>: Operation,
    E: Build<SubtractOperation<A>>,
{
    type Output = E::Output;

    fn subtract(&self, argument: A) -> Self::Output {
        self.build(SubtractOperation { argument })
    }
}

operation_manifest! {
    SubtractOperation<A> {
        method: Subtract<A>::subtract;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueSubtract>;
            argument: A: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueSubtract + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
