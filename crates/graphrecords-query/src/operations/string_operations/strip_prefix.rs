use super::{string_rebuild_argument_map_bare, string_rebuild_argument_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueString,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::StripPrefix,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "StripPrefix")]
#[plan(optimizer_hints(empty = if_all))]
pub struct StripPrefixOperation<A> {
    #[argument]
    prefix: A,
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for StripPrefixOperation<A>
where
    I: IndexDomain,
    V: ValueString,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: ValueString,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_rebuild_argument_map_indexed::<I, V, A>(
            graphrecord,
            prepared,
            |value, prefix, _| {
                let value = match value.strip_prefix(prefix) {
                    Some(stripped) => stripped.to_string(),
                    None => value.to_string(),
                };

                Ok(value)
            },
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, A> ElementKernel<Bare<V>> for StripPrefixOperation<A>
where
    V: ValueString + BareValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: ValueString,
{
    type Emission = A::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_rebuild_argument_map_bare::<V, A>(
            graphrecord,
            prepared,
            |value, prefix, _| {
                let value = match value.strip_prefix(prefix) {
                    Some(stripped) => stripped.to_string(),
                    None => value.to_string(),
                };

                Ok(value)
            },
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E, A> StripPrefix<A> for E
where
    StripPrefixOperation<A>: Operation,
    E: Build<StripPrefixOperation<A>>,
{
    type Output = E::Output;

    fn strip_prefix(&self, prefix: A) -> Self::Output {
        self.build(StripPrefixOperation { prefix })
    }
}

operation_manifest! {
    StripPrefixOperation<A> {
        method: StripPrefix<A>::strip_prefix;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueString>;
            argument: A: ArgumentSource<Keyed<I>> where A::ValueDomain: ValueString;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueString + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned> where A::ValueDomain: ValueString;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
