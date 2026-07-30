use super::{string_argument_map_bare, string_argument_map_indexed};
use crate::{
    Bare, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::StringValue,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::StripSuffix,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "StripSuffix")]
#[plan(optimizer_hints(empty = if_all))]
pub struct StripSuffixOperation<A> {
    #[argument]
    suffix: A,
}

impl<A: Prepare> Prepare for StripSuffixOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.suffix.prepare(graphrecord, cache)
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for StripSuffixOperation<A>
where
    I: IndexDomain,
    V: StringValue,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_argument_map_indexed::<_, V, V, A>(
            prepared,
            Self::LABEL,
            |_, value, suffix| {
                let value = match value.strip_suffix(&suffix) {
                    Some(stripped) => stripped.to_string(),
                    None => value,
                };

                Ok(V::from_string(value))
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, A> ElementKernel<Bare<V>> for StripSuffixOperation<A>
where
    V: StringValue,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    type Emission = A::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_argument_map_bare::<V, V, A>(
            prepared,
            Self::LABEL,
            |_, value, suffix| {
                let value = match value.strip_suffix(&suffix) {
                    Some(stripped) => stripped.to_string(),
                    None => value,
                };

                Ok(V::from_string(value))
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O, A> StripSuffix<A> for O
where
    StripSuffixOperation<A>: Operation,
    O: Apply<StripSuffixOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn strip_suffix(&self, suffix: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            StripSuffixOperation { suffix },
        ))
    }
}
