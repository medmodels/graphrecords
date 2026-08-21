use crate::{
    Bare, BareValueDomain, Diagnostic, ErrorGroup, Explain, Expression, Failure, FailureKind,
    FailureKindValue, FailureValue, IndexDomain, Indexed, Labeled, Mask, QueryResult, Scalar,
    Series, ValueDomain,
    element::{Dropping, Pipeline, Preserving},
    explain::ExplainFormatter,
    operations::{
        Apply, Build, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::{ErrorKind, ErrorKindName, Errors, HasErrorCause, InErrorGroup, IsErrorKind},
};
use graphrecords_core::{GraphRecord, graphrecord::ValueView};
use std::{
    any::type_name,
    borrow::Cow,
    error::Error,
    fmt::{self, Write},
    marker::PhantomData,
};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Errors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ErrorsOperation;

impl<I: IndexDomain, V: ValueDomain> ElementKernel<Indexed<I, V>> for ErrorsOperation {
    type Emission = Dropping;
    type OutShape = Indexed<I, FailureValue>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<_>| {
            result.err().map(|failure| Ok(*failure))
        }))
    }
}

impl<V: BareValueDomain> ElementKernel<Bare<V>> for ErrorsOperation {
    type Emission = Dropping;
    type OutShape = Bare<FailureValue>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<_>| {
            result.err().map(|failure| Ok(*failure))
        }))
    }
}

impl<E: Build<ErrorsOperation>> Errors for E {
    type Output = E::Output;

    fn errors(&self) -> Self::Output {
        self.build(ErrorsOperation)
    }
}

pub(super) mod errors {
    use super::{
        Bare, Dropping, Errors, ErrorsOperation, FailureValue, Indexed, operation_manifest,
    };

    operation_manifest! {
        ErrorsOperation {
            method: Errors::errors;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueDomain>;
                input: Indexed<I, V>;
                output: Indexed<I, FailureValue>;
                emission: Dropping;
            }

            kernel {
                parameters: <V: BareValueDomain>;
                input: Bare<V>;
                output: Bare<FailureValue>;
                emission: Dropping;
            }
        }
    }
}

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "ErrorKind")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct ErrorKindOperation;

impl<I: IndexDomain> ElementKernel<Indexed<I, FailureValue>> for ErrorKindOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, FailureKindValue>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, FailureValue>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<Failure>| {
            result.map(|failure| failure.kind())
        }))
    }
}

impl ElementKernel<Bare<FailureValue>> for ErrorKindOperation {
    type Emission = Preserving;
    type OutShape = Bare<FailureKindValue>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<FailureValue>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<Failure>| {
            result.map(|failure| failure.kind())
        }))
    }
}

impl<E: Build<ErrorKindOperation>> ErrorKind for E {
    type Output = E::Output;

    fn kind(&self) -> Self::Output {
        self.build(ErrorKindOperation)
    }
}

pub(super) mod kind {
    use super::{
        Bare, ErrorKind, ErrorKindOperation, FailureKindValue, FailureValue, Indexed, Preserving,
        operation_manifest,
    };

    operation_manifest! {
        ErrorKindOperation {
            method: ErrorKind::kind;
            scope: element;

            kernel {
                parameters: <I: IndexDomain>;
                input: Indexed<I, FailureValue>;
                output: Indexed<I, FailureKindValue>;
                emission: Preserving;
            }

            kernel {
                parameters: <>;
                input: Bare<FailureValue>;
                output: Bare<FailureKindValue>;
                emission: Preserving;
            }
        }
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct IsErrorKindOperation<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic> Labeled for IsErrorKindOperation<D> {
    const LABEL: &'static str = "IsErrorKind";
}

impl<D: Diagnostic> IsErrorKindOperation<D> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<D: Diagnostic> Clone for IsErrorKindOperation<D> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<D: Diagnostic> Explain for IsErrorKindOperation<D> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} kind={}", Self::LABEL, D::name())
    }
}

impl<I: IndexDomain, D: Diagnostic> ElementKernel<Indexed<I, FailureValue>>
    for IsErrorKindOperation<D>
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, FailureValue>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<Failure>| {
            result.map(|failure| failure.is_kind::<D>())
        }))
    }
}

impl<D: Diagnostic> ElementKernel<Bare<FailureValue>> for IsErrorKindOperation<D> {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<FailureValue>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<Failure>| {
            result.map(|failure| failure.is_kind::<D>())
        }))
    }
}

impl<E: Expression> IsErrorKind for E {
    type Expression = E;
    type Output<D>
        = E::Output
    where
        D: Diagnostic,
        E: Apply<IsErrorKindOperation<D>>;

    fn is<D>(&self) -> Self::Output<D>
    where
        D: Diagnostic,
        Self: Apply<IsErrorKindOperation<D>>,
    {
        Self::Output::new(OperationContext::new(
            self.clone(),
            IsErrorKindOperation::new(),
        ))
    }
}

impl<E: Expression> IsErrorKind for Series<E> {
    type Expression = E;
    type Output<D>
        = Series<E::Output>
    where
        D: Diagnostic,
        E: Apply<IsErrorKindOperation<D>>;

    fn is<D>(&self) -> Self::Output<D>
    where
        D: Diagnostic,
        E: Apply<IsErrorKindOperation<D>>,
    {
        self.bind(self.expression().is())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct InErrorGroupOperation<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup> Labeled for InErrorGroupOperation<G> {
    const LABEL: &'static str = "InErrorGroup";
}

impl<G: ErrorGroup> InErrorGroupOperation<G> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<G: ErrorGroup> Clone for InErrorGroupOperation<G> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<G: ErrorGroup> Explain for InErrorGroupOperation<G> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} group={}", Self::LABEL, G::name())
    }
}

impl<I: IndexDomain, G: ErrorGroup> ElementKernel<Indexed<I, FailureValue>>
    for InErrorGroupOperation<G>
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, FailureValue>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<Failure>| {
            result.map(|failure| G::contains(&failure.kind()))
        }))
    }
}

impl<G: ErrorGroup> ElementKernel<Bare<FailureValue>> for InErrorGroupOperation<G> {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<FailureValue>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<Failure>| {
            result.map(|failure| G::contains(&failure.kind()))
        }))
    }
}

impl<E: Expression> InErrorGroup for E {
    type Expression = E;
    type Output<G>
        = E::Output
    where
        G: ErrorGroup,
        E: Apply<InErrorGroupOperation<G>>;

    fn in_error_group<G>(&self) -> Self::Output<G>
    where
        G: ErrorGroup,
        Self: Apply<InErrorGroupOperation<G>>,
    {
        Self::Output::new(OperationContext::new(
            self.clone(),
            InErrorGroupOperation::new(),
        ))
    }
}

impl<E: Expression> InErrorGroup for Series<E> {
    type Expression = E;
    type Output<G>
        = Series<E::Output>
    where
        G: ErrorGroup,
        E: Apply<InErrorGroupOperation<G>>;

    fn in_error_group<G>(&self) -> Self::Output<G>
    where
        G: ErrorGroup,
        E: Apply<InErrorGroupOperation<G>>,
    {
        self.bind(self.expression().in_error_group())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct HasErrorCauseOperation<E: Error + 'static> {
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static> Labeled for HasErrorCauseOperation<E> {
    const LABEL: &'static str = "HasErrorCause";
}

impl<E: Error + 'static> HasErrorCauseOperation<E> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<E: Error + 'static> Clone for HasErrorCauseOperation<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Error + 'static> Explain for HasErrorCauseOperation<E> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} cause={}", Self::LABEL, type_name::<E>())
    }
}

impl<I: IndexDomain, E: Error + 'static> ElementKernel<Indexed<I, FailureValue>>
    for HasErrorCauseOperation<E>
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, FailureValue>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<Failure>| {
            result.map(|failure| failure.has_cause::<E>())
        }))
    }
}

impl<E: Error + 'static> ElementKernel<Bare<FailureValue>> for HasErrorCauseOperation<E> {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<FailureValue>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<Failure>| {
            result.map(|failure| failure.has_cause::<E>())
        }))
    }
}

impl<E: Expression> HasErrorCause for E {
    type Expression = E;
    type Output<C>
        = E::Output
    where
        C: Error + 'static,
        E: Apply<HasErrorCauseOperation<C>>;

    fn has_cause<C>(&self) -> Self::Output<C>
    where
        C: Error + 'static,
        Self: Apply<HasErrorCauseOperation<C>>,
    {
        Self::Output::new(OperationContext::new(
            self.clone(),
            HasErrorCauseOperation::new(),
        ))
    }
}

impl<E: Expression> HasErrorCause for Series<E> {
    type Expression = E;
    type Output<C>
        = Series<E::Output>
    where
        C: Error + 'static,
        E: Apply<HasErrorCauseOperation<C>>;

    fn has_cause<C>(&self) -> Self::Output<C>
    where
        C: Error + 'static,
        E: Apply<HasErrorCauseOperation<C>>,
    {
        self.bind(self.expression().has_cause())
    }
}

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "ErrorKindName")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_any))]
pub struct ErrorKindNameOperation;

impl<I: IndexDomain> ElementKernel<Indexed<I, FailureKindValue>> for ErrorKindNameOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, FailureKindValue>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<FailureKind>| {
            result.map(|kind| ValueView::String(Cow::Borrowed(kind.name())))
        }))
    }
}

impl ElementKernel<Bare<FailureKindValue>> for ErrorKindNameOperation {
    type Emission = Preserving;
    type OutShape = Bare<Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<FailureKindValue>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<FailureKind>| {
            result.map(|kind| ValueView::String(Cow::Borrowed(kind.name())))
        }))
    }
}

impl<E: Build<ErrorKindNameOperation>> ErrorKindName for E {
    type Output = E::Output;

    fn name(&self) -> Self::Output {
        self.build(ErrorKindNameOperation)
    }
}

pub(super) mod name {
    use super::{
        Bare, ErrorKindName, ErrorKindNameOperation, FailureKindValue, Indexed, Preserving, Scalar,
        operation_manifest,
    };

    operation_manifest! {
        ErrorKindNameOperation {
            method: ErrorKindName::name;
            scope: element;

            kernel {
                parameters: <I: IndexDomain>;
                input: Indexed<I, FailureKindValue>;
                output: Indexed<I, Scalar>;
                emission: Preserving;
            }

            kernel {
                parameters: <>;
                input: Bare<FailureKindValue>;
                output: Bare<Scalar>;
                emission: Preserving;
            }
        }
    }
}
