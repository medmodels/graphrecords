use super::{
    BucketErrorPolicy, BucketErrorPolicyIn, BucketErrorPolicyOf, BucketErrorPolicyWithCause,
};
use crate::{
    Bare, Definite, Diagnostic, ElementShape, ErrorGroup, EvaluateExpression, Explain, Expression,
    IndexDomain, Indexed, Labeled, Multiple, QueryResult, Single,
    explain::ExplainFormatter,
    expressions::{ExpressionHandle, GroupedExpression, Partition},
    operations::{
        Apply, BucketFailureArity, GroupKernel, Operation, OperationContext, Prepare, policy::Raise,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::OnBucketError,
};
use graphrecords_core::GraphRecord;
use std::{
    any::type_name,
    error::Error,
    fmt::{self, Write},
    marker::PhantomData,
};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Group)]
#[explain(label = "RaiseBucketErrors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseBucketErrors;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseBucketErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> Labeled for RaiseBucketErrorsOf<D> {
    const LABEL: &'static str = "RaiseBucketErrorsOf";
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
        write!(formatter, "{} kind={}", Self::LABEL, D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseBucketErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> Labeled for RaiseBucketErrorsIn<G> {
    const LABEL: &'static str = "RaiseBucketErrorsIn";
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
        write!(formatter, "{} group={}", Self::LABEL, G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseBucketErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static> Labeled for RaiseBucketErrorsWithCause<E> {
    const LABEL: &'static str = "RaiseBucketErrorsWithCause";
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
        write!(formatter, "{} cause={}", Self::LABEL, type_name::<E>())
    }
}

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, B: BucketFailureArity<S>>
    GroupKernel<M, K, ExpressionHandle<S, B>> for RaiseBucketErrors
{
    type Output = GroupedExpression<M, K, ExpressionHandle<S, B>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, B>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        for bucket in partition.buckets() {
            if let Some(failure) = B::bucket_failure(bucket.payload()) {
                return Err(Box::new(failure.clone()));
            }
        }

        Ok(partition)
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, B: BucketFailureArity<S>, D: Diagnostic>
    GroupKernel<M, K, ExpressionHandle<S, B>> for RaiseBucketErrorsOf<D>
{
    type Output = GroupedExpression<M, K, ExpressionHandle<S, B>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, B>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        for bucket in partition.buckets() {
            if let Some(failure) = B::bucket_failure(bucket.payload())
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

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, B: BucketFailureArity<S>, G: ErrorGroup>
    GroupKernel<M, K, ExpressionHandle<S, B>> for RaiseBucketErrorsIn<G>
{
    type Output = GroupedExpression<M, K, ExpressionHandle<S, B>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, B>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        for bucket in partition.buckets() {
            if let Some(failure) = B::bucket_failure(bucket.payload())
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

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, B: BucketFailureArity<S>, E: Error + 'static>
    GroupKernel<M, K, ExpressionHandle<S, B>> for RaiseBucketErrorsWithCause<E>
{
    type Output = GroupedExpression<M, K, ExpressionHandle<S, B>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, B>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        for bucket in partition.buckets() {
            if let Some(failure) = B::bucket_failure(bucket.payload())
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

impl<E: Apply<RaiseBucketErrors>> BucketErrorPolicy<E> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseBucketErrors))
    }
}

impl<E: Apply<RaiseBucketErrorsOf<D>>, D: Diagnostic> BucketErrorPolicyOf<E, D> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseBucketErrorsOf::new()))
    }
}

impl<E: Apply<RaiseBucketErrorsIn<G>>, G: ErrorGroup> BucketErrorPolicyIn<E, G> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseBucketErrorsIn::new()))
    }
}

impl<E: Apply<RaiseBucketErrorsWithCause<C>>, C: Error + 'static> BucketErrorPolicyWithCause<E, C>
    for Raise
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            RaiseBucketErrorsWithCause::new(),
        ))
    }
}

operation_manifest! {
    RaiseBucketErrors as "on_bucket_error_raise" {
        method: OnBucketError::on_bucket_error;
        policy: Raise;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            input: ExpressionHandle<Indexed<I, V>, Multiple<O>>;
            output: GroupedExpression<M, K, ExpressionHandle<Indexed<I, V>, Multiple<O>>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: ExpressionHandle<Indexed<I, V>, Single>;
            output: GroupedExpression<M, K, ExpressionHandle<Indexed<I, V>, Single>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: ExpressionHandle<Indexed<I, V>, Definite>;
            output: GroupedExpression<M, K, ExpressionHandle<Indexed<I, V>, Definite>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain, O: OrderState>;
            input: ExpressionHandle<Bare<V>, Multiple<O>>;
            output: GroupedExpression<M, K, ExpressionHandle<Bare<V>, Multiple<O>>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain>;
            input: ExpressionHandle<Bare<V>, Single>;
            output: GroupedExpression<M, K, ExpressionHandle<Bare<V>, Single>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain>;
            input: ExpressionHandle<Bare<V>, Definite>;
            output: GroupedExpression<M, K, ExpressionHandle<Bare<V>, Definite>>;
        }
    }
}
