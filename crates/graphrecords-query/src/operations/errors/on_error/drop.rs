use crate::{
    Bare, BareValueDomain, Diagnostic, ErrorGroup, Explain, Expression, IndexDomain, Indexed,
    Labeled, QueryResult, ValueDomain,
    element::{Dropping, Pipeline},
    explain::ExplainFormatter,
    operations::{
        Apply, ElementKernel, ElementPipeline, ErrorPolicy, ErrorPolicyIn, ErrorPolicyOf,
        ErrorPolicyWithCause, Operation, OperationContext, Prepare,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::OnError,
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
#[operation(scope = Element)]
#[explain(label = "Drop")]
#[plan(optimizer_hints(empty = if_any))]
pub struct Drop;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> Labeled for DropErrorsOf<D> {
    const LABEL: &'static str = "DropErrorsOf";
}

impl<D: Diagnostic> DropErrorsOf<D> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<D: Diagnostic> Clone for DropErrorsOf<D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<D: Diagnostic> Explain for DropErrorsOf<D> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} kind={}", Self::LABEL, D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> Labeled for DropErrorsIn<G> {
    const LABEL: &'static str = "DropErrorsIn";
}

impl<G: ErrorGroup> DropErrorsIn<G> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<G: ErrorGroup> Clone for DropErrorsIn<G> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<G: ErrorGroup> Explain for DropErrorsIn<G> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} group={}", Self::LABEL, G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropErrorsWithCause<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static> Labeled for DropErrorsWithCause<E> {
    const LABEL: &'static str = "DropErrorsWithCause";
}

impl<E: Error + 'static> DropErrorsWithCause<E> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<E: Error + 'static> Clone for DropErrorsWithCause<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Error + 'static> Explain for DropErrorsWithCause<E> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} cause={}", Self::LABEL, type_name::<E>())
    }
}

impl<I: IndexDomain, V: ValueDomain> ElementKernel<Indexed<I, V>> for Drop {
    type Emission = Dropping;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<_>| {
            result.ok().map(Ok)
        }))
    }
}

impl<V: BareValueDomain> ElementKernel<Bare<V>> for Drop {
    type Emission = Dropping;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<_>| result.ok().map(Ok)))
    }
}

impl<I: IndexDomain, V: ValueDomain, D: Diagnostic> ElementKernel<Indexed<I, V>>
    for DropErrorsOf<D>
{
    type Emission = Dropping;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<_>| match result {
            Err(failure) if failure.is_kind::<D>() => None,
            result => Some(result),
        }))
    }
}

impl<V: BareValueDomain, D: Diagnostic> ElementKernel<Bare<V>> for DropErrorsOf<D> {
    type Emission = Dropping;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<_>| match result {
            Err(failure) if failure.is_kind::<D>() => None,
            result => Some(result),
        }))
    }
}

impl<I: IndexDomain, V: ValueDomain, G: ErrorGroup> ElementKernel<Indexed<I, V>>
    for DropErrorsIn<G>
{
    type Emission = Dropping;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<_>| match result {
            Err(failure) if G::contains(&failure.kind()) => None,
            result => Some(result),
        }))
    }
}

impl<V: BareValueDomain, G: ErrorGroup> ElementKernel<Bare<V>> for DropErrorsIn<G> {
    type Emission = Dropping;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<_>| match result {
            Err(failure) if G::contains(&failure.kind()) => None,
            result => Some(result),
        }))
    }
}

impl<I: IndexDomain, V: ValueDomain, E: Error + 'static> ElementKernel<Indexed<I, V>>
    for DropErrorsWithCause<E>
{
    type Emission = Dropping;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<_>| match result {
            Err(failure) if failure.has_cause::<E>() => None,
            result => Some(result),
        }))
    }
}

impl<V: BareValueDomain, E: Error + 'static> ElementKernel<Bare<V>> for DropErrorsWithCause<E> {
    type Emission = Dropping;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<_>| match result {
            Err(failure) if failure.has_cause::<E>() => None,
            result => Some(result),
        }))
    }
}

impl<E: Apply<Self>> ErrorPolicy<E> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, Self))
    }
}

impl<E: Apply<DropErrorsOf<D>>, D: Diagnostic> ErrorPolicyOf<E, D> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropErrorsOf::new()))
    }
}

impl<E: Apply<DropErrorsIn<G>>, G: ErrorGroup> ErrorPolicyIn<E, G> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropErrorsIn::new()))
    }
}

impl<E: Apply<DropErrorsWithCause<C>>, C: Error + 'static> ErrorPolicyWithCause<E, C> for Drop {
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropErrorsWithCause::new()))
    }
}

operation_manifest! {
    Drop as "on_error_drop" {
        method: OnError::on_error;
        policy: Drop;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: Dropping;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            input: Bare<V>;
            output: Bare<V>;
            emission: Dropping;
        }
    }
}
