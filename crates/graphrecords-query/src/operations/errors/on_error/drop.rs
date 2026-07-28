use crate::{
    Bare, Diagnostic, ErrorGroup, Explain, IndexDomain, Indexed, Operand, QueryResult, ValueType,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{
        Apply, Dropping, ElementKernel, ElementPipeline, ErrorPolicy, ErrorPolicyIn, ErrorPolicyOf,
        ErrorPolicyWithCause, Operation, OperationContext, Pipeline, Prepare,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
};
use graphrecords_core::GraphRecord;
use std::{
    any::type_name,
    error::Error,
    fmt::{self, Write},
    marker::PhantomData,
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Drop")]
pub struct Drop;

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
pub struct DropErrorsOf<D: Diagnostic> {
    marker: PhantomData<fn() -> D>,
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
        write!(formatter, "DropErrorsOf kind={}", D::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
pub struct DropErrorsIn<G: ErrorGroup> {
    marker: PhantomData<fn() -> G>,
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
        write!(formatter, "DropErrorsIn group={}", G::name())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
pub struct DropErrorsWithCause<C: Error + 'static> {
    marker: PhantomData<fn() -> C>,
}

impl<C: Error + 'static> DropErrorsWithCause<C> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<C: Error + 'static> Clone for DropErrorsWithCause<C> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<C: Error + 'static> Explain for DropErrorsWithCause<C> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "DropErrorsWithCause cause={}", type_name::<C>())
    }
}

impl Prepare for Drop {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<D: Diagnostic> Prepare for DropErrorsOf<D> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<G: ErrorGroup> Prepare for DropErrorsIn<G> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<C: Error + 'static> Prepare for DropErrorsWithCause<C> {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueType> ElementKernel<Indexed<I, V>> for Drop {
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

impl<V: ValueType> ElementKernel<Bare<V>> for Drop {
    type Emission = Dropping;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<_>| result.ok().map(Ok)))
    }
}

impl<I: IndexDomain, V: ValueType, D: Diagnostic> ElementKernel<Indexed<I, V>> for DropErrorsOf<D> {
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

impl<V: ValueType, D: Diagnostic> ElementKernel<Bare<V>> for DropErrorsOf<D> {
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

impl<I: IndexDomain, V: ValueType, G: ErrorGroup> ElementKernel<Indexed<I, V>> for DropErrorsIn<G> {
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

impl<V: ValueType, G: ErrorGroup> ElementKernel<Bare<V>> for DropErrorsIn<G> {
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

impl<I: IndexDomain, V: ValueType, C: Error + 'static> ElementKernel<Indexed<I, V>>
    for DropErrorsWithCause<C>
{
    type Emission = Dropping;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::unkeyed(|result: QueryResult<_>| match result {
            Err(failure) if failure.has_cause::<C>() => None,
            result => Some(result),
        }))
    }
}

impl<V: ValueType, C: Error + 'static> ElementKernel<Bare<V>> for DropErrorsWithCause<C> {
    type Emission = Dropping;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(|result: QueryResult<_>| match result {
            Err(failure) if failure.has_cause::<C>() => None,
            result => Some(result),
        }))
    }
}

impl<I: Apply<Self>> ErrorPolicy<I> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, Self))
    }
}

impl<I: Apply<DropErrorsOf<D>>, D: Diagnostic> ErrorPolicyOf<I, D> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropErrorsOf::new()))
    }
}

impl<I: Apply<DropErrorsIn<G>>, G: ErrorGroup> ErrorPolicyIn<I, G> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropErrorsIn::new()))
    }
}

impl<I: Apply<DropErrorsWithCause<C>>, C: Error + 'static> ErrorPolicyWithCause<I, C> for Drop {
    type Output = I::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, DropErrorsWithCause::new()))
    }
}
