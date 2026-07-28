use super::{
    BucketErrorPolicy, BucketErrorPolicyIn, BucketErrorPolicyOf, BucketErrorPolicyWithCause,
};
use crate::{
    Diagnostic, ElementShape, ErrorGroup, EvaluateOperand, Explain, IndexDomain, Operand,
    QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operands::{GroupOperand, OperandHandle, Partition},
    operations::{
        Apply, BucketFailureArity, GroupKernel, GroupKey, Operation, OperationContext, Prepare,
        Raise,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;
use std::{
    any::type_name,
    error::Error,
    fmt::{self, Write},
    marker::PhantomData,
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "RaiseBucketErrors")]
pub struct RaiseBucketErrors;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
pub struct RaiseBucketErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> RaiseBucketErrorsOf<D> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<D: Diagnostic> Clone for RaiseBucketErrorsOf<D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<D: Diagnostic> Explain for RaiseBucketErrorsOf<D> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "RaiseBucketErrorsOf kind={}", D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
pub struct RaiseBucketErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> RaiseBucketErrorsIn<G> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<G: ErrorGroup> Clone for RaiseBucketErrorsIn<G> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<G: ErrorGroup> Explain for RaiseBucketErrorsIn<G> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "RaiseBucketErrorsIn group={}", G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
pub struct RaiseBucketErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static> RaiseBucketErrorsWithCause<E> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<E: Error + 'static> Clone for RaiseBucketErrorsWithCause<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Error + 'static> Explain for RaiseBucketErrorsWithCause<E> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(
            formatter,
            "RaiseBucketErrorsWithCause cause={}",
            type_name::<E>()
        )
    }
}

impl Prepare for RaiseBucketErrors {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<D: Diagnostic> Prepare for RaiseBucketErrorsOf<D> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<G: ErrorGroup> Prepare for RaiseBucketErrorsIn<G> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<E: Error + 'static> Prepare for RaiseBucketErrorsWithCause<E> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: BucketFailureArity<S>>
    GroupKernel<M, K, OperandHandle<S, C>> for RaiseBucketErrors
{
    type Output = GroupOperand<M, K, OperandHandle<S, C>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        for bucket in partition.buckets() {
            if let Some(failure) = C::bucket_failure(bucket.payload()) {
                return Err(Box::new(failure.clone()));
            }
        }

        Ok(partition)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: BucketFailureArity<S>, D: Diagnostic>
    GroupKernel<M, K, OperandHandle<S, C>> for RaiseBucketErrorsOf<D>
{
    type Output = GroupOperand<M, K, OperandHandle<S, C>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        for bucket in partition.buckets() {
            if let Some(failure) = C::bucket_failure(bucket.payload())
                && failure.is_kind::<D>()
            {
                return Err(Box::new(failure.clone()));
            }
        }

        Ok(partition)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: BucketFailureArity<S>, G: ErrorGroup>
    GroupKernel<M, K, OperandHandle<S, C>> for RaiseBucketErrorsIn<G>
{
    type Output = GroupOperand<M, K, OperandHandle<S, C>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        for bucket in partition.buckets() {
            if let Some(failure) = C::bucket_failure(bucket.payload())
                && G::contains(&failure.kind())
            {
                return Err(Box::new(failure.clone()));
            }
        }

        Ok(partition)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: BucketFailureArity<S>, E: Error + 'static>
    GroupKernel<M, K, OperandHandle<S, C>> for RaiseBucketErrorsWithCause<E>
{
    type Output = GroupOperand<M, K, OperandHandle<S, C>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        for bucket in partition.buckets() {
            if let Some(failure) = C::bucket_failure(bucket.payload())
                && failure.has_cause::<E>()
            {
                return Err(Box::new(failure.clone()));
            }
        }

        Ok(partition)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: Apply<RaiseBucketErrors>> BucketErrorPolicy<I> for Raise {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseBucketErrors))
    }
}

impl<I: Apply<RaiseBucketErrorsOf<D>>, D: Diagnostic> BucketErrorPolicyOf<I, D> for Raise {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseBucketErrorsOf::new()))
    }
}

impl<I: Apply<RaiseBucketErrorsIn<G>>, G: ErrorGroup> BucketErrorPolicyIn<I, G> for Raise {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseBucketErrorsIn::new()))
    }
}

impl<I: Apply<RaiseBucketErrorsWithCause<E>>, E: Error + 'static> BucketErrorPolicyWithCause<I, E>
    for Raise
{
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            RaiseBucketErrorsWithCause::new(),
        ))
    }
}
