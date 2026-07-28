use super::{equality_bare, equality_indexed};
use crate::{
    AttributeName, Bare, Explain, FailureKind, FailureKindValue, IndexDomain, IndexValue, Indexed,
    Labeled, Mask, Operand, QueryResult, Scalar,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::EqualTo,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordAttribute, GraphRecordValue},
};

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

fn equal_estimate(input: Estimate) -> Estimate {
    let selectivity = input.distinct.map(|distinct| 1.0 / distinct.max(1) as f64);

    Estimate {
        distinct: None,
        selectivity,
        ..input
    }
}

impl<I, A> ElementKernel<Indexed<I, Scalar>> for EqualToOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordValue>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Scalar>, Self>> {
        Ok(equality_indexed::<_, A, Scalar>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        equal_estimate(input)
    }
}

impl<A> ElementKernel<Bare<Scalar>> for EqualToOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordValue>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Scalar>, Self>> {
        Ok(equality_bare::<A, Scalar>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        equal_estimate(input)
    }
}

impl<I, A> ElementKernel<Indexed<I, Mask>> for EqualToOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = bool>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Mask>, Self>> {
        Ok(equality_indexed::<_, A, Mask>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.argument.estimate(stats).selectivity)
            .map(|(subject, argument)| (2.0 * subject).mul_add(argument, 1.0 - subject - argument));

        Estimate {
            distinct: None,
            selectivity,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<Mask>> for EqualToOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = bool>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        Ok(equality_bare::<A, Mask>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.argument.estimate(stats).selectivity)
            .map(|(subject, argument)| (2.0 * subject).mul_add(argument, 1.0 - subject - argument));

        Estimate {
            distinct: None,
            selectivity,
            ..input
        }
    }
}

impl<I, A> ElementKernel<Indexed<I, AttributeName>> for EqualToOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordAttribute>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, AttributeName>, Self>> {
        Ok(equality_indexed::<_, A, AttributeName>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        equal_estimate(input)
    }
}

impl<A> ElementKernel<Bare<AttributeName>> for EqualToOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordAttribute>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<AttributeName>, Self>> {
        Ok(equality_bare::<A, AttributeName>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        equal_estimate(input)
    }
}

impl<K, I, A> ElementKernel<Indexed<K, IndexValue<I>>> for EqualToOperation<A>
where
    K: IndexDomain,
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<K>, Value<'a> = I::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<K, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<I>>, Self>> {
        Ok(equality_indexed::<_, A, IndexValue<I>>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        equal_estimate(input)
    }
}

impl<I, A> ElementKernel<Bare<IndexValue<I>>> for EqualToOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = I::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<I>>, Self>> {
        Ok(equality_bare::<A, IndexValue<I>>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        equal_estimate(input)
    }
}

impl<I, A> ElementKernel<Indexed<I, FailureKindValue>> for EqualToOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = FailureKind>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, FailureKindValue>, Self>> {
        Ok(equality_indexed::<_, A, FailureKindValue>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        equal_estimate(input)
    }
}

impl<A> ElementKernel<Bare<FailureKindValue>> for EqualToOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = FailureKind>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<FailureKindValue>, Self>> {
        Ok(equality_bare::<A, FailureKindValue>(
            prepared,
            Self::LABEL,
            |first, second| first == second,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        equal_estimate(input)
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
