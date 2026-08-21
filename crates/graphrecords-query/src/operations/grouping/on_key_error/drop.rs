use super::{KeyErrorPolicy, KeyErrorPolicyIn, KeyErrorPolicyOf, KeyErrorPolicyWithCause};
use crate::{
    Diagnostic, ErrorGroup, EvaluateExpression, Explain, Expression, IndexDomain, Labeled,
    QueryResult,
    explain::ExplainFormatter,
    expressions::{GroupedExpression, KeyFailureChange, Partition},
    operations::{Apply, GroupKernel, Operation, OperationContext, Prepare, policy::Drop},
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
#[explain(label = "DropKeyErrors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropKeyErrors;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropKeyErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> Labeled for DropKeyErrorsOf<D> {
    const LABEL: &'static str = "DropKeyErrorsOf";
}

impl<D: Diagnostic> DropKeyErrorsOf<D> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<D: Diagnostic> Clone for DropKeyErrorsOf<D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<D: Diagnostic> Explain for DropKeyErrorsOf<D> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} kind={}", Self::LABEL, D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropKeyErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> Labeled for DropKeyErrorsIn<G> {
    const LABEL: &'static str = "DropKeyErrorsIn";
}

impl<G: ErrorGroup> DropKeyErrorsIn<G> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<G: ErrorGroup> Clone for DropKeyErrorsIn<G> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<G: ErrorGroup> Explain for DropKeyErrorsIn<G> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} group={}", Self::LABEL, G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Group)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropKeyErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static> Labeled for DropKeyErrorsWithCause<E> {
    const LABEL: &'static str = "DropKeyErrorsWithCause";
}

impl<E: Error + 'static> DropKeyErrorsWithCause<E> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<E: Error + 'static> Clone for DropKeyErrorsWithCause<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Error + 'static> Explain for DropKeyErrorsWithCause<E> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} cause={}", Self::LABEL, type_name::<E>())
    }
}

impl<M: IndexDomain, K: IndexDomain, E: Expression> GroupKernel<M, K, E> for DropKeyErrors {
    type Output = GroupedExpression<M, K, E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, E>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        partition.change_key_failures(|_| Some(KeyFailureChange::Drop))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: IndexDomain, E: Expression, D: Diagnostic> GroupKernel<M, K, E>
    for DropKeyErrorsOf<D>
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
                Some(KeyFailureChange::Drop)
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
    for DropKeyErrorsIn<G>
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
                Some(KeyFailureChange::Drop)
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
    for DropKeyErrorsWithCause<C>
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
                Some(KeyFailureChange::Drop)
            } else {
                None
            }
        })
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<E: Apply<DropKeyErrors>> KeyErrorPolicy<E> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropKeyErrors))
    }
}

impl<E: Apply<DropKeyErrorsOf<D>>, D: Diagnostic> KeyErrorPolicyOf<E, D> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropKeyErrorsOf::new()))
    }
}

impl<E: Apply<DropKeyErrorsIn<G>>, G: ErrorGroup> KeyErrorPolicyIn<E, G> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropKeyErrorsIn::new()))
    }
}

impl<E: Apply<DropKeyErrorsWithCause<C>>, C: Error + 'static> KeyErrorPolicyWithCause<E, C>
    for Drop
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropKeyErrorsWithCause::new()))
    }
}

operation_manifest! {
    DropKeyErrors as "on_key_error_drop" {
        method: OnKeyError::on_key_error;
        policy: Drop;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <E: Lane>;
            input: E;
            output: GroupedExpression<M, K, E>;
        }
    }
}
