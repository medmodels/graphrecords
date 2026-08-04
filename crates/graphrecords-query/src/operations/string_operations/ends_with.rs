use super::{string_argument_map_bare, string_argument_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult,
    capabilities::StringValue,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::EndsWith,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "EndsWith")]
#[plan(optimizer_hints(empty = if_all))]
pub struct EndsWithOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for EndsWithOperation<A> {
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

impl<I, V, A> ElementKernel<Indexed<I, V>> for EndsWithOperation<A>
where
    I: IndexDomain,
    V: StringValue,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: StringValue,
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
            |_, value, argument| Ok(value.ends_with(&argument)),
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
    V: StringValue + BareValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: StringValue,
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
            |_, value, argument| Ok(value.ends_with(&argument)),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<O, A> EndsWith<A> for O
where
    EndsWithOperation<A>: Operation,
    O: Apply<EndsWithOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn ends_with(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            EndsWithOperation { argument },
        ))
    }
}

operation_manifest! {
    EndsWithOperation<A> {
        method: EndsWith<A>::ends_with;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: StringValue>;
            argument: A: ArgumentSource<Keyed<I>> where A::ValueDomain: StringValue;
            input: Indexed<I, V>;
            output: Indexed<I, Mask>;
            emission: ArgumentRetention;
        }
        kernel {
            parameters: <V: StringValue + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned> where A::ValueDomain: StringValue;
            input: Bare<V>;
            output: Bare<Mask>;
            emission: ArgumentRetention;
        }
    }
}
