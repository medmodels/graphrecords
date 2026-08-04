use super::{string_replace_bare, string_replace_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::StringValue,
    element::Retention,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::Replace,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Replace")]
#[plan(optimizer_hints(empty = if_all))]
pub struct ReplaceOperation<A, B> {
    #[argument]
    old: A,
    #[argument]
    new: B,
}

impl<A: Prepare, B: Prepare> Prepare for ReplaceOperation<A, B> {
    type Prepared<'a>
        = (A::Prepared<'a>, B::Prepared<'a>)
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((
            self.old.prepare(graphrecord, cache)?,
            self.new.prepare(graphrecord, cache)?,
        ))
    }
}

impl<I, V, A, B> ElementKernel<Indexed<I, V>> for ReplaceOperation<A, B>
where
    I: IndexDomain,
    V: StringValue,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: StringValue,
    B: ArgumentSource<Keyed<I>>,
    B::ValueDomain: StringValue,
{
    type Emission = <A::Retention as Retention>::Then<B::Retention>;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_replace_indexed::<_, V, A, B>(
            prepared,
            Self::LABEL,
            |value, old, new| value.replacen(old, new, 1),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, A, B> ElementKernel<Bare<V>> for ReplaceOperation<A, B>
where
    V: StringValue + BareValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: StringValue,
    B: ArgumentSource<Unaligned>,
    B::ValueDomain: StringValue,
{
    type Emission = <A::Retention as Retention>::Then<B::Retention>;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_replace_bare::<V, A, B>(
            prepared,
            Self::LABEL,
            |value, old, new| value.replacen(old, new, 1),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O, A, B> Replace<A, B> for O
where
    ReplaceOperation<A, B>: Operation,
    O: Apply<ReplaceOperation<A, B>>,
{
    type ReturnOperand = O::Output;

    fn replace(&self, old: A, new: B) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            ReplaceOperation { old, new },
        ))
    }
}

operation_manifest! {
    ReplaceOperation<A, B> {
        method: Replace<A, B>::replace;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: StringValue>;
            argument: A: ArgumentSource<Keyed<I>> where A::ValueDomain: StringValue;
            argument: B: ArgumentSource<Keyed<I>> where B::ValueDomain: StringValue;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }
        kernel {
            parameters: <V: StringValue + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned> where A::ValueDomain: StringValue;
            argument: B: ArgumentSource<Unaligned> where B::ValueDomain: StringValue;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
