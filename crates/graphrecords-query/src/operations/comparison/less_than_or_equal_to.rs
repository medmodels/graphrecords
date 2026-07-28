use super::{ordering_bare, ordering_indexed};
use crate::{
    AttributeName, Bare, Explain, FailureKind, FailureKindValue, IndexDomain, IndexValue, Indexed,
    Labeled, Mask, Operand, QueryResult, Scalar,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::LessThanOrEqualTo,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordAttribute, GraphRecordValue},
};
use std::cmp::Ordering;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "LessThanOrEqualTo")]
#[plan(optimizer_hints(empty = if_all))]
pub struct LessThanOrEqualToOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for LessThanOrEqualToOperation<A> {
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

impl<I, A> ElementKernel<Indexed<I, Scalar>> for LessThanOrEqualToOperation<A>
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
        Ok(ordering_indexed::<_, A, Scalar>(
            prepared,
            Self::LABEL,
            GraphRecordValue::partial_cmp,
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<Scalar>> for LessThanOrEqualToOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordValue>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Scalar>, Self>> {
        Ok(ordering_bare::<A, Scalar>(
            prepared,
            Self::LABEL,
            GraphRecordValue::partial_cmp,
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<I, A> ElementKernel<Indexed<I, Mask>> for LessThanOrEqualToOperation<A>
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
        Ok(ordering_indexed::<_, A, Mask>(
            prepared,
            Self::LABEL,
            |first, second| Some(first.cmp(second)),
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.argument.estimate(stats).selectivity)
            .map(|(subject, argument)| subject.mul_add(-(1.0 - argument), 1.0));

        Estimate {
            distinct: None,
            selectivity,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<Mask>> for LessThanOrEqualToOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = bool>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        Ok(ordering_bare::<A, Mask>(
            prepared,
            Self::LABEL,
            |first, second| Some(first.cmp(second)),
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.argument.estimate(stats).selectivity)
            .map(|(subject, argument)| subject.mul_add(-(1.0 - argument), 1.0));

        Estimate {
            distinct: None,
            selectivity,
            ..input
        }
    }
}

impl<I, A> ElementKernel<Indexed<I, AttributeName>> for LessThanOrEqualToOperation<A>
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
        Ok(ordering_indexed::<_, A, AttributeName>(
            prepared,
            Self::LABEL,
            GraphRecordAttribute::partial_cmp,
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<AttributeName>> for LessThanOrEqualToOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordAttribute>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<AttributeName>, Self>> {
        Ok(ordering_bare::<A, AttributeName>(
            prepared,
            Self::LABEL,
            GraphRecordAttribute::partial_cmp,
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<K, I, A> ElementKernel<Indexed<K, IndexValue<I>>> for LessThanOrEqualToOperation<A>
where
    K: IndexDomain,
    I: IndexDomain,
    I::Owned: PartialOrd,
    for<'a> A: ArgumentSource<Keyed<K>, Value<'a> = I::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<K, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<I>>, Self>> {
        Ok(ordering_indexed::<_, A, IndexValue<I>>(
            prepared,
            Self::LABEL,
            PartialOrd::partial_cmp,
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<I, A> ElementKernel<Bare<IndexValue<I>>> for LessThanOrEqualToOperation<A>
where
    I: IndexDomain,
    I::Owned: PartialOrd,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = I::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<I>>, Self>> {
        Ok(ordering_bare::<A, IndexValue<I>>(
            prepared,
            Self::LABEL,
            PartialOrd::partial_cmp,
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<I, A> ElementKernel<Indexed<I, FailureKindValue>> for LessThanOrEqualToOperation<A>
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
        Ok(ordering_indexed::<_, A, FailureKindValue>(
            prepared,
            Self::LABEL,
            PartialOrd::partial_cmp,
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<FailureKindValue>> for LessThanOrEqualToOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = FailureKind>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<FailureKindValue>, Self>> {
        Ok(ordering_bare::<A, FailureKindValue>(
            prepared,
            Self::LABEL,
            PartialOrd::partial_cmp,
            Ordering::is_le,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            selectivity: None,
            ..input
        }
    }
}

impl<O, A> LessThanOrEqualTo<A> for O
where
    LessThanOrEqualToOperation<A>: Operation,
    O: Apply<LessThanOrEqualToOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn less_than_or_equal_to(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            LessThanOrEqualToOperation { argument },
        ))
    }
}
