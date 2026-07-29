use super::{numeric_bare, numeric_indexed};
use crate::{
    Bare, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult, ValueType,
    element::Preserving,
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Round,
    value::ValueRound,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Round")]
#[plan(optimizer_hints(empty = if_any))]
pub struct RoundOperation;

impl Prepare for RoundOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V> ElementKernel<Indexed<I, V>> for RoundOperation
where
    I: IndexDomain,
    for<'a> V: ValueRound + ValueType<Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(numeric_indexed::<I, V>(Self::LABEL, V::round))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<V> ElementKernel<Bare<V>> for RoundOperation
where
    for<'a> V: ValueRound + ValueType<Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(numeric_bare::<V>(Self::LABEL, V::round))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<O: Apply<RoundOperation>> Round for O {
    type ReturnOperand = O::Output;

    fn round(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), RoundOperation))
    }
}
