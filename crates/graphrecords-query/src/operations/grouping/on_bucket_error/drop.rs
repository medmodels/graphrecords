use super::{
    BucketErrorPolicy, BucketErrorPolicyIn, BucketErrorPolicyOf, BucketErrorPolicyWithCause,
};
use crate::{
    Bare, Definite, Diagnostic, ElementShape, ErrorGroup, EvaluateOperand, Explain, IndexDomain,
    Indexed, Multiple, Operand, QueryResult, Single,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    index::GroupKey,
    operands::{BucketChange, GroupOperand, OperandHandle, Partition},
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

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "DropBucketErrors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropBucketErrors;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropBucketErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
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
        write!(formatter, "DropBucketErrorsOf kind={}", D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropBucketErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
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
        write!(formatter, "DropBucketErrorsIn group={}", G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropBucketErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
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
        write!(
            formatter,
            "DropBucketErrorsWithCause cause={}",
            type_name::<E>()
        )
    }
}

impl Prepare for DropBucketErrors {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<D: Diagnostic> Prepare for DropBucketErrorsOf<D> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<G: ErrorGroup> Prepare for DropBucketErrorsIn<G> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<E: Error + 'static> Prepare for DropBucketErrorsWithCause<E> {
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
    GroupKernel<M, K, OperandHandle<S, C>> for DropBucketErrors
{
    type Output = GroupOperand<M, K, OperandHandle<S, C>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(partition.change_buckets(|bucket| {
            C::bucket_failure(bucket.payload()).map(|_| BucketChange::Drop)
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            per_group: input.per_group,
            ..Estimate::UNKNOWN
        }
    }
}

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: BucketFailureArity<S>, D: Diagnostic>
    GroupKernel<M, K, OperandHandle<S, C>> for DropBucketErrorsOf<D>
{
    type Output = GroupOperand<M, K, OperandHandle<S, C>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(
            partition.change_buckets(|bucket| match C::bucket_failure(bucket.payload()) {
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

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: BucketFailureArity<S>, G: ErrorGroup>
    GroupKernel<M, K, OperandHandle<S, C>> for DropBucketErrorsIn<G>
{
    type Output = GroupOperand<M, K, OperandHandle<S, C>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(
            partition.change_buckets(|bucket| match C::bucket_failure(bucket.payload()) {
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

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: BucketFailureArity<S>, E: Error + 'static>
    GroupKernel<M, K, OperandHandle<S, C>> for DropBucketErrorsWithCause<E>
{
    type Output = GroupOperand<M, K, OperandHandle<S, C>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(
            partition.change_buckets(|bucket| match C::bucket_failure(bucket.payload()) {
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

impl<I: Apply<DropBucketErrors>> BucketErrorPolicy<I> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropBucketErrors))
    }
}

impl<I: Apply<DropBucketErrorsOf<D>>, D: Diagnostic> BucketErrorPolicyOf<I, D> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropBucketErrorsOf::new()))
    }
}

impl<I: Apply<DropBucketErrorsIn<G>>, G: ErrorGroup> BucketErrorPolicyIn<I, G> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropBucketErrorsIn::new()))
    }
}

impl<I: Apply<DropBucketErrorsWithCause<E>>, E: Error + 'static> BucketErrorPolicyWithCause<I, E>
    for Drop
{
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
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
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            input: OperandHandle<Indexed<I, V>, Multiple<O>>;
            output: GroupOperand<M, K, OperandHandle<Indexed<I, V>, Multiple<O>>>;
        }

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: OperandHandle<Indexed<I, V>, Single>;
            output: GroupOperand<M, K, OperandHandle<Indexed<I, V>, Single>>;
        }

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: OperandHandle<Indexed<I, V>, Definite>;
            output: GroupOperand<M, K, OperandHandle<Indexed<I, V>, Definite>>;
        }

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <V: BareValueDomain, O: OrderState>;
            input: OperandHandle<Bare<V>, Multiple<O>>;
            output: GroupOperand<M, K, OperandHandle<Bare<V>, Multiple<O>>>;
        }

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <V: BareValueDomain>;
            input: OperandHandle<Bare<V>, Single>;
            output: GroupOperand<M, K, OperandHandle<Bare<V>, Single>>;
        }

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <V: BareValueDomain>;
            input: OperandHandle<Bare<V>, Definite>;
            output: GroupOperand<M, K, OperandHandle<Bare<V>, Definite>>;
        }
    }
}
