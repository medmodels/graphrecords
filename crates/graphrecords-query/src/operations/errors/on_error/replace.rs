use crate::{
    Bare, Diagnostic, ErrorGroup, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    ValueType,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, ErrorPolicy, ErrorPolicyIn,
        ErrorPolicyOf, ErrorPolicyWithCause, Keyed, Operation, OperationContext, Pipeline, Prepare,
        Retention, Unaligned,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
};
use graphrecords_core::GraphRecord;
use std::{any::type_name, error::Error, fmt, marker::PhantomData};

#[derive(Clone, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct Replace<A>(#[argument] pub A);

impl<A> Labeled for Replace<A> {
    const LABEL: &'static str = "Replace";
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct ReplaceErrorsOf<D: Diagnostic, R> {
    #[argument]
    replacement: R,
    marker: PhantomData<fn() -> D>,
}

impl<D: Diagnostic, R> Labeled for ReplaceErrorsOf<D, R> {
    const LABEL: &'static str = "ReplaceErrorsOf";
}

impl<D: Diagnostic, R> ReplaceErrorsOf<D, R> {
    const fn new(replacement: R) -> Self {
        Self {
            replacement,
            marker: PhantomData,
        }
    }
}

impl<D: Diagnostic, R: Clone> Clone for ReplaceErrorsOf<D, R> {
    fn clone(&self) -> Self {
        Self::new(self.replacement.clone())
    }
}

impl<D: Diagnostic, R: Explain> Explain for ReplaceErrorsOf<D, R> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        fmt::Write::write_fmt(
            formatter,
            format_args!("ReplaceErrorsOf kind={}", D::name()),
        )?;
        formatter.labeled_child("replacement", &self.replacement);

        Ok(())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct ReplaceErrorsIn<G: ErrorGroup, R> {
    #[argument]
    replacement: R,
    marker: PhantomData<fn() -> G>,
}

impl<G: ErrorGroup, R> Labeled for ReplaceErrorsIn<G, R> {
    const LABEL: &'static str = "ReplaceErrorsIn";
}

impl<G: ErrorGroup, R> ReplaceErrorsIn<G, R> {
    const fn new(replacement: R) -> Self {
        Self {
            replacement,
            marker: PhantomData,
        }
    }
}

impl<G: ErrorGroup, R: Clone> Clone for ReplaceErrorsIn<G, R> {
    fn clone(&self) -> Self {
        Self::new(self.replacement.clone())
    }
}

impl<G: ErrorGroup, R: Explain> Explain for ReplaceErrorsIn<G, R> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        fmt::Write::write_fmt(
            formatter,
            format_args!("ReplaceErrorsIn group={}", G::name()),
        )?;
        formatter.labeled_child("replacement", &self.replacement);

        Ok(())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct ReplaceErrorsWithCause<C: Error + 'static, R> {
    #[argument]
    replacement: R,
    marker: PhantomData<fn() -> C>,
}

impl<C: Error + 'static, R> Labeled for ReplaceErrorsWithCause<C, R> {
    const LABEL: &'static str = "ReplaceErrorsWithCause";
}

impl<C: Error + 'static, R> ReplaceErrorsWithCause<C, R> {
    const fn new(replacement: R) -> Self {
        Self {
            replacement,
            marker: PhantomData,
        }
    }
}

impl<C: Error + 'static, R: Clone> Clone for ReplaceErrorsWithCause<C, R> {
    fn clone(&self) -> Self {
        Self::new(self.replacement.clone())
    }
}

impl<C: Error + 'static, R: Explain> Explain for ReplaceErrorsWithCause<C, R> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        fmt::Write::write_fmt(
            formatter,
            format_args!("ReplaceErrorsWithCause cause={}", type_name::<C>()),
        )?;
        formatter.labeled_child("replacement", &self.replacement);

        Ok(())
    }
}

impl<A: Explain> Explain for Replace<A> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        fmt::Write::write_str(formatter, "Replace")?;
        formatter.labeled_child("replacement", &self.0);

        Ok(())
    }
}

impl<R: Prepare> Prepare for Replace<R> {
    type Prepared<'a>
        = R::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.0.prepare(graphrecord, cache)
    }
}

impl<D: Diagnostic, R: Prepare> Prepare for ReplaceErrorsOf<D, R> {
    type Prepared<'a>
        = R::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.replacement.prepare(graphrecord, cache)
    }
}

impl<G: ErrorGroup, R: Prepare> Prepare for ReplaceErrorsIn<G, R> {
    type Prepared<'a>
        = R::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.replacement.prepare(graphrecord, cache)
    }
}

impl<C: Error + 'static, R: Prepare> Prepare for ReplaceErrorsWithCause<C, R> {
    type Prepared<'a>
        = R::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.replacement.prepare(graphrecord, cache)
    }
}

impl<I, V, R> ElementKernel<Indexed<I, V>> for Replace<R>
where
    I: IndexDomain,
    V: ValueType,
    for<'a> R: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type OutShape = Indexed<I, V>;
    type Retention = <R as ArgumentSource<Keyed<I>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |(index, result): (I::Index<'a>, QueryResult<V::Value<'a>>)| match result {
                Ok(value) => <Self::Retention as Retention>::keep((index, Ok(value))),
                Err(original) => {
                    let step = R::resolve(&prepared, &index, label);

                    <Self::Retention as Retention>::map_step(step, |replacement| {
                        let result = match replacement {
                            Ok(value) => Ok(value),
                            Err(_) => Err(original),
                        };

                        (index, result)
                    })
                }
            },
        ))
    }
}

impl<V, R> ElementKernel<Bare<V>> for Replace<R>
where
    V: ValueType,
    for<'a> R: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    type OutShape = Bare<V>;
    type Retention = <R as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |result: QueryResult<V::Value<'a>>| match result {
                Ok(value) => <Self::Retention as Retention>::keep(Ok(value)),
                Err(original) => {
                    let step = R::resolve(&prepared, &(), label);

                    <Self::Retention as Retention>::map_step(
                        step,
                        |replacement| match replacement {
                            Ok(value) => Ok(value),
                            Err(_) => Err(original),
                        },
                    )
                }
            },
        ))
    }
}

impl<I, V, D, R> ElementKernel<Indexed<I, V>> for ReplaceErrorsOf<D, R>
where
    I: IndexDomain,
    V: ValueType,
    D: Diagnostic,
    for<'a> R: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type OutShape = Indexed<I, V>;
    type Retention = <R as ArgumentSource<Keyed<I>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |(index, result): (I::Index<'a>, QueryResult<V::Value<'a>>)| match result {
                Err(original) if original.is_kind::<D>() => {
                    let step = R::resolve(&prepared, &index, label);

                    <Self::Retention as Retention>::map_step(step, |replacement| {
                        let result = match replacement {
                            Ok(value) => Ok(value),
                            Err(_) => Err(original),
                        };

                        (index, result)
                    })
                }
                result => <Self::Retention as Retention>::keep((index, result)),
            },
        ))
    }
}

impl<V, D, R> ElementKernel<Bare<V>> for ReplaceErrorsOf<D, R>
where
    V: ValueType,
    D: Diagnostic,
    for<'a> R: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    type OutShape = Bare<V>;
    type Retention = <R as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |result: QueryResult<V::Value<'a>>| match result {
                Err(original) if original.is_kind::<D>() => {
                    let step = R::resolve(&prepared, &(), label);

                    <Self::Retention as Retention>::map_step(
                        step,
                        |replacement| match replacement {
                            Ok(value) => Ok(value),
                            Err(_) => Err(original),
                        },
                    )
                }
                result => <Self::Retention as Retention>::keep(result),
            },
        ))
    }
}

impl<I, V, G, R> ElementKernel<Indexed<I, V>> for ReplaceErrorsIn<G, R>
where
    I: IndexDomain,
    V: ValueType,
    G: ErrorGroup,
    for<'a> R: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type OutShape = Indexed<I, V>;
    type Retention = <R as ArgumentSource<Keyed<I>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |(index, result): (I::Index<'a>, QueryResult<V::Value<'a>>)| match result {
                Err(original) if G::contains(&original.kind()) => {
                    let step = R::resolve(&prepared, &index, label);

                    <Self::Retention as Retention>::map_step(step, |replacement| {
                        let result = match replacement {
                            Ok(value) => Ok(value),
                            Err(_) => Err(original),
                        };

                        (index, result)
                    })
                }
                result => <Self::Retention as Retention>::keep((index, result)),
            },
        ))
    }
}

impl<V, G, R> ElementKernel<Bare<V>> for ReplaceErrorsIn<G, R>
where
    V: ValueType,
    G: ErrorGroup,
    for<'a> R: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    type OutShape = Bare<V>;
    type Retention = <R as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |result: QueryResult<V::Value<'a>>| match result {
                Err(original) if G::contains(&original.kind()) => {
                    let step = R::resolve(&prepared, &(), label);

                    <Self::Retention as Retention>::map_step(
                        step,
                        |replacement| match replacement {
                            Ok(value) => Ok(value),
                            Err(_) => Err(original),
                        },
                    )
                }
                result => <Self::Retention as Retention>::keep(result),
            },
        ))
    }
}

impl<I, V, C, R> ElementKernel<Indexed<I, V>> for ReplaceErrorsWithCause<C, R>
where
    I: IndexDomain,
    V: ValueType,
    C: Error + 'static,
    for<'a> R: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type OutShape = Indexed<I, V>;
    type Retention = <R as ArgumentSource<Keyed<I>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |(index, result): (I::Index<'a>, QueryResult<V::Value<'a>>)| match result {
                Err(original) if original.has_cause::<C>() => {
                    let step = R::resolve(&prepared, &index, label);

                    <Self::Retention as Retention>::map_step(step, |replacement| {
                        let result = match replacement {
                            Ok(value) => Ok(value),
                            Err(_) => Err(original),
                        };

                        (index, result)
                    })
                }
                result => <Self::Retention as Retention>::keep((index, result)),
            },
        ))
    }
}

impl<V, C, R> ElementKernel<Bare<V>> for ReplaceErrorsWithCause<C, R>
where
    V: ValueType,
    C: Error + 'static,
    for<'a> R: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    type OutShape = Bare<V>;
    type Retention = <R as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |result: QueryResult<V::Value<'a>>| match result {
                Err(original) if original.has_cause::<C>() => {
                    let step = R::resolve(&prepared, &(), label);

                    <Self::Retention as Retention>::map_step(
                        step,
                        |replacement| match replacement {
                            Ok(value) => Ok(value),
                            Err(_) => Err(original),
                        },
                    )
                }
                result => <Self::Retention as Retention>::keep(result),
            },
        ))
    }
}

impl<I, A> ErrorPolicy<I> for Replace<A>
where
    A: Clone + 'static,
    Self: Operation,
    I: Apply<Self>,
{
    type Output = <I as Apply<Self>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, self.clone()))
    }
}

impl<I, D, A> ErrorPolicyOf<I, D> for Replace<A>
where
    D: Diagnostic,
    A: Clone + 'static,
    ReplaceErrorsOf<D, A>: Operation,
    I: Apply<ReplaceErrorsOf<D, A>>,
{
    type Output = <I as Apply<ReplaceErrorsOf<D, A>>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            ReplaceErrorsOf::new(self.0.clone()),
        ))
    }
}

impl<I, G, A> ErrorPolicyIn<I, G> for Replace<A>
where
    G: ErrorGroup,
    A: Clone + 'static,
    ReplaceErrorsIn<G, A>: Operation,
    I: Apply<ReplaceErrorsIn<G, A>>,
{
    type Output = <I as Apply<ReplaceErrorsIn<G, A>>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            ReplaceErrorsIn::new(self.0.clone()),
        ))
    }
}

impl<I, C, A> ErrorPolicyWithCause<I, C> for Replace<A>
where
    C: Error + 'static,
    A: Clone + 'static,
    ReplaceErrorsWithCause<C, A>: Operation,
    I: Apply<ReplaceErrorsWithCause<C, A>>,
{
    type Output = <I as Apply<ReplaceErrorsWithCause<C, A>>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            ReplaceErrorsWithCause::new(self.0.clone()),
        ))
    }
}
