use crate::{
    Bare, BareValueDomain, Diagnostic, ErrorGroup, Explain, Failure, FailureKind, FailureKindValue,
    FailureValue, IndexDomain, Indexed, Mask, Operand, QueryResult, Scalar, ValueDomain,
    element::{Dropping, Pipeline, Preserving},
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::{ErrorKind, ErrorKindName, Errors, HasErrorCause, InErrorGroup, IsErrorKind},
};
use graphrecords_core::{GraphRecord, graphrecord::Value};
use std::{
    any::type_name,
    error::Error,
    fmt::{self, Write},
    marker::PhantomData,
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Errors")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ErrorsOperation;

impl Prepare for ErrorsOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

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

impl<O: Apply<ErrorsOperation>> Errors for O {
    type ReturnOperand = O::Output;

    fn errors(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ErrorsOperation))
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

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ErrorKind")]
#[plan(optimizer_hints(
    commutes_with_filter,
    allows_limit_pushdown,
    empty = if_any
))]
pub struct ErrorKindOperation;

impl Prepare for ErrorKindOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

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

impl<O: Apply<ErrorKindOperation>> ErrorKind for O {
    type ReturnOperand = O::Output;

    fn kind(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ErrorKindOperation))
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

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[plan(optimizer_hints(
    commutes_with_filter,
    allows_limit_pushdown,
    empty = if_any
))]
pub struct IsErrorKindOperation<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
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
        write!(formatter, "IsErrorKind kind={}", D::name())
    }
}

impl<D: Diagnostic> Prepare for IsErrorKindOperation<D> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
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

impl<O: Operand> IsErrorKind for O {
    type ReturnOperand<D>
        = O::Output
    where
        D: Diagnostic,
        O: Apply<IsErrorKindOperation<D>>;

    fn is<D>(&self) -> Self::ReturnOperand<D>
    where
        D: Diagnostic,
        Self: Apply<IsErrorKindOperation<D>>,
    {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            IsErrorKindOperation::new(),
        ))
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[plan(optimizer_hints(
    commutes_with_filter,
    allows_limit_pushdown,
    empty = if_any
))]
pub struct InErrorGroupOperation<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
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
        write!(formatter, "InErrorGroup group={}", G::name())
    }
}

impl<G: ErrorGroup> Prepare for InErrorGroupOperation<G> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
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

impl<O: Operand> InErrorGroup for O {
    type ReturnOperand<G>
        = O::Output
    where
        G: ErrorGroup,
        O: Apply<InErrorGroupOperation<G>>;

    fn in_error_group<G>(&self) -> Self::ReturnOperand<G>
    where
        G: ErrorGroup,
        Self: Apply<InErrorGroupOperation<G>>,
    {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            InErrorGroupOperation::new(),
        ))
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[plan(optimizer_hints(
    commutes_with_filter,
    allows_limit_pushdown,
    empty = if_any
))]
pub struct HasErrorCauseOperation<C: Error + 'static> {
    marker: PhantomData<fn() -> C>,
}

impl<C: Error + 'static> HasErrorCauseOperation<C> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<C: Error + 'static> Clone for HasErrorCauseOperation<C> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<C: Error + 'static> Explain for HasErrorCauseOperation<C> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "HasErrorCause cause={}", type_name::<C>())
    }
}

impl<C: Error + 'static> Prepare for HasErrorCauseOperation<C> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, C: Error + 'static> ElementKernel<Indexed<I, FailureValue>>
    for HasErrorCauseOperation<C>
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, FailureValue>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<Failure>| {
            result.map(|failure| failure.has_cause::<C>())
        }))
    }
}

impl<C: Error + 'static> ElementKernel<Bare<FailureValue>> for HasErrorCauseOperation<C> {
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<FailureValue>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<Failure>| {
            result.map(|failure| failure.has_cause::<C>())
        }))
    }
}

impl<O: Operand> HasErrorCause for O {
    type ReturnOperand<C>
        = O::Output
    where
        C: Error + 'static,
        O: Apply<HasErrorCauseOperation<C>>;

    fn has_cause<C>(&self) -> Self::ReturnOperand<C>
    where
        C: Error + 'static,
        Self: Apply<HasErrorCauseOperation<C>>,
    {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            HasErrorCauseOperation::new(),
        ))
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ErrorKindName")]
#[plan(optimizer_hints(
    commutes_with_filter,
    allows_limit_pushdown,
    empty = if_any
))]
pub struct ErrorKindNameOperation;

impl Prepare for ErrorKindNameOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, FailureKindValue>> for ErrorKindNameOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, FailureKindValue>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<FailureKind>| {
            result.map(|kind| Value::from(kind.name()))
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
            result.map(|kind| Value::from(kind.name()))
        }))
    }
}

impl<O: Apply<ErrorKindNameOperation>> ErrorKindName for O {
    type ReturnOperand = O::Output;

    fn name(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ErrorKindNameOperation))
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
