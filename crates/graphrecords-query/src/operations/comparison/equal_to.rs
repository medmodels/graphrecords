use super::{equality_bare, equality_indexed};
use crate::value::ValueEquality;
use crate::{
    Bare, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult, ValueType,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
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
    for<'a> V: ValueEquality + ValueType<Value<'a> = <V as ValueType>::Owned>,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(equality_indexed::<_, A, V>(prepared, Self::LABEL, V::equal))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        let selectivity = input.distinct.map(|distinct| 1.0 / distinct.max(1) as f64);

        Estimate {
            distinct: None,
            selectivity,
            ..input
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for EqualToOperation<A>
where
    for<'a> V: ValueEquality + ValueType<Value<'a> = <V as ValueType>::Owned>,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(equality_bare::<A, V>(prepared, Self::LABEL, V::equal))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        let selectivity = input.distinct.map(|distinct| 1.0 / distinct.max(1) as f64);

        Estimate {
            distinct: None,
            selectivity,
            ..input
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
