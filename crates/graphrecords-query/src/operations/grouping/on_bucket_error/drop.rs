use super::{
    BucketErrorPolicy, BucketErrorPolicyIn, BucketErrorPolicyOf, BucketErrorPolicyWithCause,
};
use crate::{
    Bare, Definite, Diagnostic, ElementShape, ErrorGroup, EvaluateExpression, Explain, Expression,
    IndexDomain, Indexed, Labeled, Multiple, QueryResult, Single,
    explain::ExplainFormatter,
    expressions::{BucketChange, ExpressionHandle, GroupedExpression, Partition},
    operations::{
        Apply, BucketFailureArity, GroupKernel, Operation, OperationContext, Prepare, policy::Drop,
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
#[explain(label = "DropBucketErrors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropBucketErrors;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropBucketErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> Labeled for DropBucketErrorsOf<D> {
    const LABEL: &'static str = "DropBucketErrorsOf";
}

impl<D: Diagnostic> DropBucketErrorsOf<D> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<D: Diagnostic> Clone for DropBucketErrorsOf<D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<D: Diagnostic> Explain for DropBucketErrorsOf<D> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} kind={}", Self::LABEL, D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropBucketErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> Labeled for DropBucketErrorsIn<G> {
    const LABEL: &'static str = "DropBucketErrorsIn";
}

impl<G: ErrorGroup> DropBucketErrorsIn<G> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<G: ErrorGroup> Clone for DropBucketErrorsIn<G> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<G: ErrorGroup> Explain for DropBucketErrorsIn<G> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} group={}", Self::LABEL, G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropBucketErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static> Labeled for DropBucketErrorsWithCause<E> {
    const LABEL: &'static str = "DropBucketErrorsWithCause";
}

impl<E: Error + 'static> DropBucketErrorsWithCause<E> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<E: Error + 'static> Clone for DropBucketErrorsWithCause<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Error + 'static> Explain for DropBucketErrorsWithCause<E> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} cause={}", Self::LABEL, type_name::<E>())
    }
}

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, B: BucketFailureArity<S>>
    GroupKernel<M, K, ExpressionHandle<S, B>> for DropBucketErrors
{
    type Output = GroupedExpression<M, K, ExpressionHandle<S, B>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, B>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(partition.change_buckets(|bucket| {
            B::bucket_failure(bucket.payload()).map(|_| BucketChange::Drop)
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            per_group: input.per_group,
            ..Estimate::UNKNOWN
        }
    }
}

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, B: BucketFailureArity<S>, D: Diagnostic>
    GroupKernel<M, K, ExpressionHandle<S, B>> for DropBucketErrorsOf<D>
{
    type Output = GroupedExpression<M, K, ExpressionHandle<S, B>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, B>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(
            partition.change_buckets(|bucket| match B::bucket_failure(bucket.payload()) {
                Some(failure) if failure.is_kind::<D>() => Some(BucketChange::Drop),
                None | Some(_) => None,
            }),
        )
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            per_group: input.per_group,
            ..Estimate::UNKNOWN
        }
    }
}

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, B: BucketFailureArity<S>, G: ErrorGroup>
    GroupKernel<M, K, ExpressionHandle<S, B>> for DropBucketErrorsIn<G>
{
    type Output = GroupedExpression<M, K, ExpressionHandle<S, B>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, B>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(
            partition.change_buckets(|bucket| match B::bucket_failure(bucket.payload()) {
                Some(failure) if G::contains(&failure.kind()) => Some(BucketChange::Drop),
                None | Some(_) => None,
            }),
        )
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            per_group: input.per_group,
            ..Estimate::UNKNOWN
        }
    }
}

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, B: BucketFailureArity<S>, E: Error + 'static>
    GroupKernel<M, K, ExpressionHandle<S, B>> for DropBucketErrorsWithCause<E>
{
    type Output = GroupedExpression<M, K, ExpressionHandle<S, B>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, B>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(
            partition.change_buckets(|bucket| match B::bucket_failure(bucket.payload()) {
                Some(failure) if failure.has_cause::<E>() => Some(BucketChange::Drop),
                None | Some(_) => None,
            }),
        )
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            per_group: input.per_group,
            ..Estimate::UNKNOWN
        }
    }
}

impl<E: Apply<DropBucketErrors>> BucketErrorPolicy<E> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropBucketErrors))
    }
}

impl<E: Apply<DropBucketErrorsOf<D>>, D: Diagnostic> BucketErrorPolicyOf<E, D> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropBucketErrorsOf::new()))
    }
}

impl<E: Apply<DropBucketErrorsIn<G>>, G: ErrorGroup> BucketErrorPolicyIn<E, G> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropBucketErrorsIn::new()))
    }
}

impl<E: Apply<DropBucketErrorsWithCause<C>>, C: Error + 'static> BucketErrorPolicyWithCause<E, C>
    for Drop
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            DropBucketErrorsWithCause::new(),
        ))
    }
}

operation_manifest! {
    DropBucketErrors as "on_bucket_error_drop" {
        method: OnBucketError::on_bucket_error;
        policy: Drop;
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
