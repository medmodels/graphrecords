use super::{arithmetic_bare, arithmetic_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueDivide,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::Divide,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Divide")]
#[plan(optimizer_hints(empty = if_all))]
pub struct DivideOperation<A> {
    #[argument]
    argument: A,
}

impl<I: IndexDomain, V: ValueDivide, A: ArgumentSource<Keyed<I>, V>> ElementKernel<Indexed<I, V>>
    for DivideOperation<A>
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
            V::divide,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V: ValueDivide + BareValueDomain, A: ArgumentSource<Unaligned, V>> ElementKernel<Bare<V>>
    for DivideOperation<A>
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
            V::divide,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E, A> Divide<A> for E
where
    DivideOperation<A>: Operation,
    E: Build<DivideOperation<A>>,
{
    type Output = E::Output;

    fn divide(&self, argument: A) -> Self::Output {
        self.build(DivideOperation { argument })
    }
}

operation_manifest! {
    DivideOperation<A> {
        method: Divide<A>::divide;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueDivide>;
            argument: A: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueDivide + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
