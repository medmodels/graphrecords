use super::{string_argument_map_bare, string_argument_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Mask, QueryResult,
    capabilities::ValueString,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::EndsWith,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "EndsWith")]
#[plan(optimizer_hints(empty = if_all))]
pub struct EndsWithOperation<A> {
    #[argument]
    argument: A,
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for EndsWithOperation<A>
where
    I: IndexDomain,
    V: ValueString,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: ValueString,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_argument_map_indexed::<_, V, Mask, A>(
            graphrecord,
            prepared,
            |value, argument, _| Ok(value.ends_with(argument)),
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

impl<V, A> ElementKernel<Bare<V>> for EndsWithOperation<A>
where
    V: ValueString + BareValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: ValueString,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_argument_map_bare::<V, Mask, A>(
            graphrecord,
            prepared,
            |value, argument, _| Ok(value.ends_with(argument)),
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

impl<E, A> EndsWith<A> for E
where
    EndsWithOperation<A>: Operation,
    E: Build<EndsWithOperation<A>>,
{
    type Output = E::Output;

    fn ends_with(&self, argument: A) -> Self::Output {
        self.build(EndsWithOperation { argument })
    }
}

operation_manifest! {
    EndsWithOperation<A> {
        method: EndsWith<A>::ends_with;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueString>;
            argument: A: ArgumentSource<Keyed<I>> where A::ValueDomain: ValueString;
            input: Indexed<I, V>;
            output: Indexed<I, Mask>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueString + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned> where A::ValueDomain: ValueString;
            input: Bare<V>;
            output: Bare<Mask>;
            emission: ArgumentRetention;
        }
    }
}
