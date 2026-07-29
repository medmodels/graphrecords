use crate::{
    Bare, Explain, IndexDomain, Indexed, Mask, Operand, QueryResult, ValueType,
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{
        Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare, SetSource,
    },
    optimizer::{
        Estimate, Estimated, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
    },
    traits::IsIn,
    value::ValueEquality,
};
use graphrecords_core::GraphRecord;
use std::hash::Hash;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "IsIn")]
#[plan(optimizer_hints(empty = if_all))]
pub struct IsInOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for IsInOperation<A> {
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

fn membership_estimate<A: Estimated>(
    operation: &IsInOperation<A>,
    input: Estimate,
    stats: &Stats,
) -> Estimate {
    let selectivity = input
        .distinct
        .zip(operation.argument.estimate(stats).elements)
        .map(|(distinct, size)| (size as f64 / distinct.max(1) as f64).min(1.0));

    Estimate {
        selectivity,
        ..input.with_unknown_distinct()
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for IsInOperation<A>
where
    I: IndexDomain,
    for<'a> V: ValueEquality + ValueType<Value<'a> = <V as ValueType>::Owned>,
    for<'a> A: SetSource<Value<'a> = <V as ValueType>::Owned>,
    V::Owned: Eq + Hash,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::unkeyed(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<V, A> ElementKernel<Bare<V>> for IsInOperation<A>
where
    for<'a> V: ValueEquality + ValueType<Value<'a> = <V as ValueType>::Owned>,
    for<'a> A: SetSource<Value<'a> = <V as ValueType>::Owned>,
    V::Owned: Eq + Hash,
{
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<O, A> IsIn<A> for O
where
    IsInOperation<A>: Operation,
    O: Apply<IsInOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn is_in(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            IsInOperation { argument },
        ))
    }
}
