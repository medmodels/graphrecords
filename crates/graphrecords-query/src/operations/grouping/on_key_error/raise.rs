use super::{KeyErrorPolicy, KeyErrorPolicyIn, KeyErrorPolicyOf, KeyErrorPolicyWithCause};
use crate::{
    Diagnostic, ErrorGroup, EvaluateOperand, Explain, IndexDomain, Operand, QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    index::GroupKey,
    operands::{GroupOperand, KeyFailureChange, Partition},
    operations::{Apply, GroupKernel, Operation, OperationContext, Prepare, policy::Raise},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::OnKeyError,
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
#[explain(label = "RaiseKeyErrors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseKeyErrors;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseKeyErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> RaiseKeyErrorsOf<D> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<D: Diagnostic> Clone for RaiseKeyErrorsOf<D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<D: Diagnostic> Explain for RaiseKeyErrorsOf<D> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "RaiseKeyErrorsOf kind={}", D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseKeyErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> RaiseKeyErrorsIn<G> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<G: ErrorGroup> Clone for RaiseKeyErrorsIn<G> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<G: ErrorGroup> Explain for RaiseKeyErrorsIn<G> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "RaiseKeyErrorsIn group={}", G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseKeyErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static> RaiseKeyErrorsWithCause<E> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<E: Error + 'static> Clone for RaiseKeyErrorsWithCause<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Error + 'static> Explain for RaiseKeyErrorsWithCause<E> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(
            formatter,
            "RaiseKeyErrorsWithCause cause={}",
            type_name::<E>()
        )
    }
}

impl Prepare for RaiseKeyErrors {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<D: Diagnostic> Prepare for RaiseKeyErrorsOf<D> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<G: ErrorGroup> Prepare for RaiseKeyErrorsIn<G> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<E: Error + 'static> Prepare for RaiseKeyErrorsWithCause<E> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<M: IndexDomain, K: GroupKey, O: Operand> GroupKernel<M, K, O> for RaiseKeyErrors {
    type Output = GroupOperand<M, K, O>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, O>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        partition.change_key_failures(|_| Some(KeyFailureChange::Raise))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: GroupKey, O: Operand, D: Diagnostic> GroupKernel<M, K, O>
    for RaiseKeyErrorsOf<D>
{
    type Output = GroupOperand<M, K, O>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, O>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        partition.change_key_failures(|key_failure| {
            let failure = key_failure.failure();

            if failure.is_kind::<D>() {
                Some(KeyFailureChange::Raise)
            } else {
                None
            }
        })
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: GroupKey, O: Operand, G: ErrorGroup> GroupKernel<M, K, O>
    for RaiseKeyErrorsIn<G>
{
    type Output = GroupOperand<M, K, O>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, O>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        partition.change_key_failures(|key_failure| {
            let failure = key_failure.failure();

            if G::contains(&failure.kind()) {
                Some(KeyFailureChange::Raise)
            } else {
                None
            }
        })
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: GroupKey, O: Operand, E: Error + 'static> GroupKernel<M, K, O>
    for RaiseKeyErrorsWithCause<E>
{
    type Output = GroupOperand<M, K, O>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, O>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        partition.change_key_failures(|key_failure| {
            let failure = key_failure.failure();

            if failure.has_cause::<E>() {
                Some(KeyFailureChange::Raise)
            } else {
                None
            }
        })
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<I: Apply<RaiseKeyErrors>> KeyErrorPolicy<I> for Raise {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseKeyErrors))
    }
}

impl<I: Apply<RaiseKeyErrorsOf<D>>, D: Diagnostic> KeyErrorPolicyOf<I, D> for Raise {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseKeyErrorsOf::new()))
    }
}

impl<I: Apply<RaiseKeyErrorsIn<G>>, G: ErrorGroup> KeyErrorPolicyIn<I, G> for Raise {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseKeyErrorsIn::new()))
    }
}

impl<I: Apply<RaiseKeyErrorsWithCause<E>>, E: Error + 'static> KeyErrorPolicyWithCause<I, E>
    for Raise
{
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseKeyErrorsWithCause::new()))
    }
}

operation_manifest! {
    RaiseKeyErrors as "on_key_error_raise" {
        method: OnKeyError::on_key_error;
        policy: Raise;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <O: Lane>;
            input: O;
            output: GroupOperand<M, K, O>;
        }
    }
}
