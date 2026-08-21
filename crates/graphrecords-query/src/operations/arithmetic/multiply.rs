use super::{arithmetic_bare, arithmetic_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueMultiply,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::Multiply,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Multiply")]
#[plan(optimizer_hints(empty = if_all))]
pub struct MultiplyOperation<A> {
    #[argument]
    argument: A,
}

impl<I: IndexDomain, V: ValueMultiply, A: ArgumentSource<Keyed<I>, V>> ElementKernel<Indexed<I, V>>
    for MultiplyOperation<A>
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
            V::multiply,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V: ValueMultiply + BareValueDomain, A: ArgumentSource<Unaligned, V>> ElementKernel<Bare<V>>
    for MultiplyOperation<A>
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
            V::multiply,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E, A> Multiply<A> for E
where
    MultiplyOperation<A>: Operation,
    E: Build<MultiplyOperation<A>>,
{
    type Output = E::Output;

    fn multiply(&self, argument: A) -> Self::Output {
        self.build(MultiplyOperation { argument })
    }
}

operation_manifest! {
    MultiplyOperation<A> {
        method: Multiply<A>::multiply;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueMultiply>;
            argument: A: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueMultiply + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
