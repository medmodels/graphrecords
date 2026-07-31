use super::{equality_bare, equality_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult,
    capabilities::ValueEquality,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::NotEqualTo,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "NotEqualTo")]
#[plan(optimizer_hints(empty = if_all))]
pub struct NotEqualToOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for NotEqualToOperation<A> {
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

impl<I, V, A> ElementKernel<Indexed<I, V>> for NotEqualToOperation<A>
where
    I: IndexDomain,
    V: ValueEquality,
    A: ArgumentSource<Keyed<I>, V>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(equality_indexed::<_, V, A>(
            prepared,
            Self::LABEL,
            |value, argument| !V::equal(value, argument),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        let selectivity = input
            .distinct
            .map(|distinct| 1.0 - 1.0 / distinct.max(1) as f64);

        Estimate {
            selectivity,
            ..input.with_unknown_distinct()
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for NotEqualToOperation<A>
where
    V: ValueEquality + BareValueDomain,
    A: ArgumentSource<Unaligned, V>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(equality_bare::<V, A>(
            prepared,
            Self::LABEL,
            |value, argument| !V::equal(value, argument),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        let selectivity = input
            .distinct
            .map(|distinct| 1.0 - 1.0 / distinct.max(1) as f64);

        Estimate {
            selectivity,
            ..input.with_unknown_distinct()
        }
    }
}

impl<O, A> NotEqualTo<A> for O
where
    NotEqualToOperation<A>: Operation,
    O: Apply<NotEqualToOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn not_equal_to(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            NotEqualToOperation { argument },
        ))
    }
}

operation_manifest! {
    NotEqualToOperation<A> {
        method: NotEqualTo<A>::not_equal_to;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueEquality>;
            argument: A: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, Mask>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueEquality + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<Mask>;
            emission: ArgumentRetention;
        }
    }
}
