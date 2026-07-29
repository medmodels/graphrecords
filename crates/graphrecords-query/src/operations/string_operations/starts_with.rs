use super::{string_argument_map_bare, string_argument_map_indexed};
use crate::{
    Bare, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::StartsWith,
    value::StringValue,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "StartsWith")]
#[plan(optimizer_hints(empty = if_all))]
pub struct StartsWithOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for StartsWithOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.argument.prepare(graphrecord, cache)
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for StartsWithOperation<A>
where
    I: IndexDomain,
    V: StringValue,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_argument_map_indexed::<_, V, Mask, A>(
            prepared,
            Self::LABEL,
            |_, value, argument| Ok(value.starts_with(&argument)),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for StartsWithOperation<A>
where
    V: StringValue,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_argument_map_bare::<V, Mask, A>(
            prepared,
            Self::LABEL,
            |_, value, argument| Ok(value.starts_with(&argument)),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<O, A> StartsWith<A> for O
where
    StartsWithOperation<A>: Operation,
    O: Apply<StartsWithOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn starts_with(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            StartsWithOperation { argument },
        ))
    }
}
