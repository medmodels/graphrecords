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
    traits::EqualTo,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "EqualTo")]
#[plan(optimizer_hints(empty = if_all))]
pub struct EqualToOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for EqualToOperation<A> {
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

impl<I, V, A> ElementKernel<Indexed<I, V>> for EqualToOperation<A>
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
        Ok(equality_indexed::<_, V, A>(prepared, Self::LABEL, V::equal))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        let selectivity = input.distinct.map(|distinct| 1.0 / distinct.max(1) as f64);

        Estimate {
            selectivity,
            ..input.with_unknown_distinct()
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for EqualToOperation<A>
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
        Ok(equality_bare::<V, A>(prepared, Self::LABEL, V::equal))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        let selectivity = input.distinct.map(|distinct| 1.0 / distinct.max(1) as f64);

        Estimate {
            selectivity,
            ..input.with_unknown_distinct()
        }
    }
}

impl<O, A> EqualTo<A> for O
where
    EqualToOperation<A>: Operation,
    O: Apply<EqualToOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn equal_to(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            EqualToOperation { argument },
        ))
    }
}

operation_manifest! {
    EqualToOperation<A> {
        method: EqualTo<A>::equal_to;
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
