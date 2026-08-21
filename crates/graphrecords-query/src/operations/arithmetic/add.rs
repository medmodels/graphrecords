use super::{arithmetic_bare, arithmetic_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueAdd,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::Add,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Add")]
#[plan(optimizer_hints(empty = if_all))]
pub struct AddOperation<A> {
    #[argument]
    argument: A,
}

impl<I: IndexDomain, V: ValueAdd, A: ArgumentSource<Keyed<I>, V>> ElementKernel<Indexed<I, V>>
    for AddOperation<A>
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
            V::add,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V: ValueAdd + BareValueDomain, A: ArgumentSource<Unaligned, V>> ElementKernel<Bare<V>>
    for AddOperation<A>
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
            V::add,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E, A> Add<A> for E
where
    AddOperation<A>: Operation,
    E: Build<AddOperation<A>>,
{
    type Output = E::Output;

    fn add(&self, argument: A) -> Self::Output {
        self.build(AddOperation { argument })
    }
}

operation_manifest! {
    AddOperation<A> {
        method: Add<A>::add;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueAdd>;
            argument: A: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueAdd + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
