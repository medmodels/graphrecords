use super::{string_replace_bare, string_replace_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueString,
    element::Retention,
    execution::EvaluationCache,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::ReplaceAll,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ReplaceAll")]
#[plan(optimizer_hints(empty = if_all))]
pub struct ReplaceAllOperation<A, B> {
    #[argument]
    old: A,
    #[argument]
    new: B,
}

impl<A: Prepare, B: Prepare> Prepare for ReplaceAllOperation<A, B> {
    type Prepared<'a>
        = (A::Prepared<'a>, B::Prepared<'a>)
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((
            self.old.prepare(graphrecord, cache)?,
            self.new.prepare(graphrecord, cache)?,
        ))
    }
}

impl<I, V, A, B> ElementKernel<Indexed<I, V>> for ReplaceAllOperation<A, B>
where
    I: IndexDomain,
    V: ValueString,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: ValueString,
    B: ArgumentSource<Keyed<I>>,
    B::ValueDomain: ValueString,
{
    type Emission = <A::Retention as Retention>::Then<B::Retention>;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_replace_indexed::<_, V, A, B>(
            graphrecord,
            prepared,
            |value, old, new| value.replace(old, new),
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, A, B> ElementKernel<Bare<V>> for ReplaceAllOperation<A, B>
where
    V: ValueString + BareValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: ValueString,
    B: ArgumentSource<Unaligned>,
    B::ValueDomain: ValueString,
{
    type Emission = <A::Retention as Retention>::Then<B::Retention>;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_replace_bare::<V, A, B>(
            graphrecord,
            prepared,
            |value, old, new| value.replace(old, new),
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E, A, B> ReplaceAll<A, B> for E
where
    ReplaceAllOperation<A, B>: Operation,
    E: Build<ReplaceAllOperation<A, B>>,
{
    type Output = E::Output;

    fn replace_all(&self, old: A, new: B) -> Self::Output {
        self.build(ReplaceAllOperation { old, new })
    }
}

operation_manifest! {
    ReplaceAllOperation<A, B> {
        method: ReplaceAll<A, B>::replace_all;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueString>;
            argument: A: ArgumentSource<Keyed<I>> where A::ValueDomain: ValueString;
            argument: B: ArgumentSource<Keyed<I>> where B::ValueDomain: ValueString;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueString + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned> where A::ValueDomain: ValueString;
            argument: B: ArgumentSource<Unaligned> where B::ValueDomain: ValueString;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
