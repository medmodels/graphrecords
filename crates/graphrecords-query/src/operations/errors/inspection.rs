use crate::{
    Bare, Diagnostic, ErrorGroup, Explain, Failure, FailureKind, FailureKindValue, FailureValue,
    IndexDomain, Indexed, Mask, Operand, QueryResult, Scalar, ValueType,
    element::{Dropping, Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::{ErrorKind, ErrorKindName, Errors, HasErrorCause, InErrorGroup, IsErrorKind},
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::{any::type_name, error::Error, marker::PhantomData};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Errors")]
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

impl<I: IndexDomain, V: ValueType> ElementKernel<Indexed<I, V>> for ErrorsOperation {
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

impl<V: ValueType> ElementKernel<Bare<V>> for ErrorsOperation {
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

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ErrorKind")]
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

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
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
    fn describe<'a>(
        &'a self,
        formatter: &mut crate::explain::ExplainFormatter<'a, '_>,
    ) -> std::fmt::Result {
        std::fmt::Write::write_fmt(formatter, format_args!("IsErrorKind kind={}", D::name()))
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
    fn describe<'a>(
        &'a self,
        formatter: &mut crate::explain::ExplainFormatter<'a, '_>,
    ) -> std::fmt::Result {
        std::fmt::Write::write_fmt(formatter, format_args!("InErrorGroup group={}", G::name()))
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
    fn describe<'a>(
        &'a self,
        formatter: &mut crate::explain::ExplainFormatter<'a, '_>,
    ) -> std::fmt::Result {
        std::fmt::Write::write_fmt(
            formatter,
            format_args!("HasErrorCause cause={}", type_name::<C>()),
        )
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
            result.map(|kind| GraphRecordValue::from(kind.name()))
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
            result.map(|kind| GraphRecordValue::from(kind.name()))
        }))
    }
}

impl<O: Apply<ErrorKindNameOperation>> ErrorKindName for O {
    type ReturnOperand = O::Output;

    fn name(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ErrorKindNameOperation))
    }
}
