use super::{KeyErrorPolicy, KeyErrorPolicyIn, KeyErrorPolicyOf, KeyErrorPolicyWithCause};
use crate::{
    Diagnostic, ErrorGroup, EvaluateExpression, Explain, Expression, IndexDomain, Labeled,
    QueryResult,
    explain::ExplainFormatter,
    expressions::{GroupedExpression, KeyFailureChange, Partition},
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

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Group)]
#[explain(label = "RaiseKeyErrors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseKeyErrors;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseKeyErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> Labeled for RaiseKeyErrorsOf<D> {
    const LABEL: &'static str = "RaiseKeyErrorsOf";
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
        write!(formatter, "{} kind={}", Self::LABEL, D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseKeyErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> Labeled for RaiseKeyErrorsIn<G> {
    const LABEL: &'static str = "RaiseKeyErrorsIn";
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
        write!(formatter, "{} group={}", Self::LABEL, G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct RaiseKeyErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static> Labeled for RaiseKeyErrorsWithCause<E> {
    const LABEL: &'static str = "RaiseKeyErrorsWithCause";
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
        write!(formatter, "{} cause={}", Self::LABEL, type_name::<E>())
    }
}

impl<M: IndexDomain, K: IndexDomain, E: Expression> GroupKernel<M, K, E> for RaiseKeyErrors {
    type Output = GroupedExpression<M, K, E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, E>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        partition.change_key_failures(|_| Some(KeyFailureChange::Raise))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: IndexDomain, E: Expression, D: Diagnostic> GroupKernel<M, K, E>
    for RaiseKeyErrorsOf<D>
{
    type Output = GroupedExpression<M, K, E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, E>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
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

impl<M: IndexDomain, K: IndexDomain, E: Expression, G: ErrorGroup> GroupKernel<M, K, E>
    for RaiseKeyErrorsIn<G>
{
    type Output = GroupedExpression<M, K, E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, E>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
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

impl<M: IndexDomain, K: IndexDomain, E: Expression, C: Error + 'static> GroupKernel<M, K, E>
    for RaiseKeyErrorsWithCause<C>
{
    type Output = GroupedExpression<M, K, E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, E>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        partition.change_key_failures(|key_failure| {
            let failure = key_failure.failure();

            if failure.has_cause::<C>() {
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

impl<E: Apply<RaiseKeyErrors>> KeyErrorPolicy<E> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseKeyErrors))
    }
}

impl<E: Apply<RaiseKeyErrorsOf<D>>, D: Diagnostic> KeyErrorPolicyOf<E, D> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseKeyErrorsOf::new()))
    }
}

impl<E: Apply<RaiseKeyErrorsIn<G>>, G: ErrorGroup> KeyErrorPolicyIn<E, G> for Raise {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseKeyErrorsIn::new()))
    }
}

impl<E: Apply<RaiseKeyErrorsWithCause<C>>, C: Error + 'static> KeyErrorPolicyWithCause<E, C>
    for Raise
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, RaiseKeyErrorsWithCause::new()))
    }
}

operation_manifest! {
    RaiseKeyErrors as "on_key_error_raise" {
        method: OnKeyError::on_key_error;
        policy: Raise;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <E: Lane>;
            input: E;
            output: GroupedExpression<M, K, E>;
        }
    }
}
