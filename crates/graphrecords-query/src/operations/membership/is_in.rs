use crate::{
    AttributeName, Bare, Explain, FailureKind, FailureKindValue, IndexDomain, IndexValue, Indexed,
    Mask, Operand, QueryResult, Scalar,
    execution::EvaluationCache,
    operations::{
        Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Pipeline, Prepare,
        Preserving, SetSource,
    },
    optimizer::{
        Estimate, Estimated, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
    },
    traits::IsIn,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordAttribute, GraphRecordValue},
};

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
        distinct: None,
        selectivity,
        ..input
    }
}

impl<I, A> ElementKernel<Indexed<I, Scalar>> for IsInOperation<A>
where
    I: IndexDomain,
    for<'a> A: SetSource<Value<'a> = GraphRecordValue>,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Scalar>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::unkeyed(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<A> ElementKernel<Bare<Scalar>> for IsInOperation<A>
where
    for<'a> A: SetSource<Value<'a> = GraphRecordValue>,
{
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Scalar>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<I, A> ElementKernel<Indexed<I, Mask>> for IsInOperation<A>
where
    I: IndexDomain,
    for<'a> A: SetSource<Value<'a> = bool>,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Mask>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::unkeyed(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<A> ElementKernel<Bare<Mask>> for IsInOperation<A>
where
    for<'a> A: SetSource<Value<'a> = bool>,
{
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<I, A> ElementKernel<Indexed<I, AttributeName>> for IsInOperation<A>
where
    I: IndexDomain,
    for<'a> A: SetSource<Value<'a> = GraphRecordAttribute>,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, AttributeName>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::unkeyed(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<A> ElementKernel<Bare<AttributeName>> for IsInOperation<A>
where
    for<'a> A: SetSource<Value<'a> = GraphRecordAttribute>,
{
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<AttributeName>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<K, I, A> ElementKernel<Indexed<K, IndexValue<I>>> for IsInOperation<A>
where
    K: IndexDomain,
    I: IndexDomain,
    for<'a> A: SetSource<Value<'a> = I::Owned>,
{
    type Emission = Preserving;
    type OutShape = Indexed<K, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<I>>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::unkeyed(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<I, A> ElementKernel<Bare<IndexValue<I>>> for IsInOperation<A>
where
    I: IndexDomain,
    for<'a> A: SetSource<Value<'a> = I::Owned>,
{
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<I>>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<I, A> ElementKernel<Indexed<I, FailureKindValue>> for IsInOperation<A>
where
    I: IndexDomain,
    for<'a> A: SetSource<Value<'a> = FailureKind>,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, FailureKindValue>, Self>> {
        let set = A::set(prepared)?;

        Ok(Pipeline::unkeyed(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<A> ElementKernel<Bare<FailureKindValue>> for IsInOperation<A>
where
    for<'a> A: SetSource<Value<'a> = FailureKind>,
{
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<FailureKindValue>, Self>> {
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
