use super::{string_rebuild_argument_map_bare, string_rebuild_argument_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::StringValue,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::StripPrefix,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "StripPrefix")]
#[plan(optimizer_hints(empty = if_all))]
pub struct StripPrefixOperation<A> {
    #[argument]
    prefix: A,
}

impl<A: Prepare> Prepare for StripPrefixOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.prefix.prepare(graphrecord, cache)
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for StripPrefixOperation<A>
where
    I: IndexDomain,
    V: StringValue,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: StringValue,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_rebuild_argument_map_indexed::<I, V, A>(
            prepared,
            Self::LABEL,
            |_, value, prefix| {
                let value = match value.strip_prefix(&prefix) {
                    Some(stripped) => stripped.to_string(),
                    None => value,
                };

                Ok(value)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, A> ElementKernel<Bare<V>> for StripPrefixOperation<A>
where
    V: StringValue + BareValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: StringValue,
{
    type Emission = A::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_rebuild_argument_map_bare::<V, A>(
            prepared,
            Self::LABEL,
            |_, value, prefix| {
                let value = match value.strip_prefix(&prefix) {
                    Some(stripped) => stripped.to_string(),
                    None => value,
                };

                Ok(value)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O, A> StripPrefix<A> for O
where
    StripPrefixOperation<A>: Operation,
    O: Apply<StripPrefixOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn strip_prefix(&self, prefix: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            StripPrefixOperation { prefix },
        ))
    }
}

operation_manifest! {
    StripPrefixOperation<A> {
        method: StripPrefix<A>::strip_prefix;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: StringValue>;
            argument: A: ArgumentSource<Keyed<I>> where A::ValueDomain: StringValue;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }
        kernel {
            parameters: <V: StringValue + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned> where A::ValueDomain: StringValue;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
