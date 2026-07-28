use super::{KeyErrorPolicy, KeyErrorPolicyIn, KeyErrorPolicyOf, KeyErrorPolicyWithCause};
use crate::{
    Diagnostic, ErrorGroup, EvaluateOperand, Explain, IndexDomain, Operand, QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operands::{GroupOperand, KeyFailureChange, Partition},
    operations::{Apply, Drop, GroupKernel, GroupKey, Operation, OperationContext, Prepare},
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
#[explain(label = "DropKeyErrors")]
pub struct DropKeyErrors;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
pub struct DropKeyErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
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
        write!(formatter, "DropKeyErrorsOf kind={}", D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
pub struct DropKeyErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
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
        write!(formatter, "DropKeyErrorsIn group={}", G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
pub struct DropKeyErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
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
        write!(
            formatter,
            "DropKeyErrorsWithCause cause={}",
            type_name::<E>()
        )
    }
}

impl Prepare for DropKeyErrors {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<D: Diagnostic> Prepare for DropKeyErrorsOf<D> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<G: ErrorGroup> Prepare for DropKeyErrorsIn<G> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<E: Error + 'static> Prepare for DropKeyErrorsWithCause<E> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<M: IndexDomain, K: GroupKey, O: Operand> GroupKernel<M, K, O> for DropKeyErrors {
    type Output = GroupOperand<M, K, O>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, O>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        partition.change_key_failures(|_| Some(KeyFailureChange::Drop))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input
    }
}

impl<M: IndexDomain, K: GroupKey, O: Operand, D: Diagnostic> GroupKernel<M, K, O>
    for DropKeyErrorsOf<D>
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

impl<M: IndexDomain, K: GroupKey, O: Operand, G: ErrorGroup> GroupKernel<M, K, O>
    for DropKeyErrorsIn<G>
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

impl<M: IndexDomain, K: GroupKey, O: Operand, E: Error + 'static> GroupKernel<M, K, O>
    for DropKeyErrorsWithCause<E>
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

impl<I: Apply<DropKeyErrors>> KeyErrorPolicy<I> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropKeyErrors))
    }
}

impl<I: Apply<DropKeyErrorsOf<D>>, D: Diagnostic> KeyErrorPolicyOf<I, D> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropKeyErrorsOf::new()))
    }
}

impl<I: Apply<DropKeyErrorsIn<G>>, G: ErrorGroup> KeyErrorPolicyIn<I, G> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropKeyErrorsIn::new()))
    }
}

impl<I: Apply<DropKeyErrorsWithCause<E>>, E: Error + 'static> KeyErrorPolicyWithCause<I, E>
    for Drop
{
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropKeyErrorsWithCause::new()))
    }
}
