use crate::{
    Bare, BareValueDomain, Diagnostic, ErrorGroup, Explain, Expression, IndexDomain, Indexed,
    Labeled, QueryResult, ValueDomain,
    element::{Pipeline, Retention},
    explain::ExplainFormatter,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, ErrorPolicy, ErrorPolicyIn,
        ErrorPolicyOf, ErrorPolicyWithCause, Keyed, Operation, OperationContext, Prepare,
        Unaligned,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::OnError,
};
use graphrecords_core::GraphRecord;
use std::{
    any::type_name,
    error::Error,
    fmt::{self, Write},
    marker::PhantomData,
};

#[derive(Clone, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(empty = if_all))]
pub struct Replace<R> {
    #[argument]
    replacement: R,
}

impl<R> Replace<R> {
    #[must_use]
    pub const fn new(replacement: R) -> Self {
        Self { replacement }
    }
}

impl<R> Labeled for Replace<R> {
    const LABEL: &'static str = "Replace";
}

impl<R: Explain> Explain for Replace<R> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.write_str(Self::LABEL)?;
        formatter.labeled_child(&self.replacement, "replacement");

        Ok(())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(empty = if_all))]
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
        Self {
            replacement: self.replacement.clone(),
            marker: PhantomData,
        }
    }
}

impl<D: Diagnostic, R: Explain> Explain for ReplaceErrorsOf<D, R> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} kind={}", Self::LABEL, D::name())?;
        formatter.labeled_child(&self.replacement, "replacement");

        Ok(())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(empty = if_all))]
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
        Self {
            replacement: self.replacement.clone(),
            marker: PhantomData,
        }
    }
}

impl<G: ErrorGroup, R: Explain> Explain for ReplaceErrorsIn<G, R> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} group={}", Self::LABEL, G::name())?;
        formatter.labeled_child(&self.replacement, "replacement");

        Ok(())
    }
}

#[derive(Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare)]
#[operation(scope = Element)]
#[plan(optimizer_hints(empty = if_all))]
pub struct ReplaceErrorsWithCause<E: Error + 'static, R> {
    #[argument]
    replacement: R,
    marker: PhantomData<fn() -> E>,
}

impl<E: Error + 'static, R> Labeled for ReplaceErrorsWithCause<E, R> {
    const LABEL: &'static str = "ReplaceErrorsWithCause";
}

impl<E: Error + 'static, R> ReplaceErrorsWithCause<E, R> {
    const fn new(replacement: R) -> Self {
        Self {
            replacement,
            marker: PhantomData,
        }
    }
}

impl<E: Error + 'static, R: Clone> Clone for ReplaceErrorsWithCause<E, R> {
    fn clone(&self) -> Self {
        Self {
            replacement: self.replacement.clone(),
            marker: PhantomData,
        }
    }
}

impl<E: Error + 'static, R: Explain> Explain for ReplaceErrorsWithCause<E, R> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{} cause={}", Self::LABEL, type_name::<E>())?;
        formatter.labeled_child(&self.replacement, "replacement");

        Ok(())
    }
}

impl<I, V, R> ElementKernel<Indexed<I, V>> for Replace<R>
where
    I: IndexDomain,
    V: ValueDomain,
    R: ArgumentSource<Keyed<I>, V>,
{
    type Emission = R::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::keyed(move |address, result| match result {
            Ok(value) => Self::Emission::keep(Ok(value)),
            Err(original) => {
                let step = R::resolve(graphrecord, &prepared, &address, label);

                Self::Emission::map_step(step, |replacement| match replacement {
                    Ok(value) => Ok(value),
                    Err(_) => Err(original),
                })
            }
        }))
    }
}

impl<V, R> ElementKernel<Bare<V>> for Replace<R>
where
    V: BareValueDomain,
    R: ArgumentSource<Unaligned, V>,
{
    type Emission = R::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::new(move |result| match result {
            Ok(value) => Self::Emission::keep(Ok(value)),
            Err(original) => {
                let step = R::resolve(graphrecord, &prepared, &(), label);

                Self::Emission::map_step(step, |replacement| match replacement {
                    Ok(value) => Ok(value),
                    Err(_) => Err(original),
                })
            }
        }))
    }
}

impl<I, V, D, R> ElementKernel<Indexed<I, V>> for ReplaceErrorsOf<D, R>
where
    I: IndexDomain,
    V: ValueDomain,
    D: Diagnostic,
    R: ArgumentSource<Keyed<I>, V>,
{
    type Emission = R::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::keyed(
            move |address, result: QueryResult<_>| match result {
                Err(original) if original.is_kind::<D>() => {
                    let step = R::resolve(graphrecord, &prepared, &address, label);

                    Self::Emission::map_step(step, |replacement| match replacement {
                        Ok(value) => Ok(value),
                        Err(_) => Err(original),
                    })
                }
                result => Self::Emission::keep(result),
            },
        ))
    }
}

impl<V, D, R> ElementKernel<Bare<V>> for ReplaceErrorsOf<D, R>
where
    V: BareValueDomain,
    D: Diagnostic,
    R: ArgumentSource<Unaligned, V>,
{
    type Emission = R::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::new(move |result: QueryResult<_>| match result {
            Err(original) if original.is_kind::<D>() => {
                let step = R::resolve(graphrecord, &prepared, &(), label);

                Self::Emission::map_step(step, |replacement| match replacement {
                    Ok(value) => Ok(value),
                    Err(_) => Err(original),
                })
            }
            result => Self::Emission::keep(result),
        }))
    }
}

impl<I, V, G, R> ElementKernel<Indexed<I, V>> for ReplaceErrorsIn<G, R>
where
    I: IndexDomain,
    V: ValueDomain,
    G: ErrorGroup,
    R: ArgumentSource<Keyed<I>, V>,
{
    type Emission = R::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::keyed(
            move |address, result: QueryResult<_>| match result {
                Err(original) if G::contains(&original.kind()) => {
                    let step = R::resolve(graphrecord, &prepared, &address, label);

                    Self::Emission::map_step(step, |replacement| match replacement {
                        Ok(value) => Ok(value),
                        Err(_) => Err(original),
                    })
                }
                result => Self::Emission::keep(result),
            },
        ))
    }
}

impl<V, G, R> ElementKernel<Bare<V>> for ReplaceErrorsIn<G, R>
where
    V: BareValueDomain,
    G: ErrorGroup,
    R: ArgumentSource<Unaligned, V>,
{
    type Emission = R::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::new(move |result: QueryResult<_>| match result {
            Err(original) if G::contains(&original.kind()) => {
                let step = R::resolve(graphrecord, &prepared, &(), label);

                Self::Emission::map_step(step, |replacement| match replacement {
                    Ok(value) => Ok(value),
                    Err(_) => Err(original),
                })
            }
            result => Self::Emission::keep(result),
        }))
    }
}

impl<I, V, E, R> ElementKernel<Indexed<I, V>> for ReplaceErrorsWithCause<E, R>
where
    I: IndexDomain,
    V: ValueDomain,
    E: Error + 'static,
    R: ArgumentSource<Keyed<I>, V>,
{
    type Emission = R::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::keyed(
            move |address, result: QueryResult<_>| match result {
                Err(original) if original.has_cause::<E>() => {
                    let step = R::resolve(graphrecord, &prepared, &address, label);

                    Self::Emission::map_step(step, |replacement| match replacement {
                        Ok(value) => Ok(value),
                        Err(_) => Err(original),
                    })
                }
                result => Self::Emission::keep(result),
            },
        ))
    }
}

impl<V, E, R> ElementKernel<Bare<V>> for ReplaceErrorsWithCause<E, R>
where
    V: BareValueDomain,
    E: Error + 'static,
    R: ArgumentSource<Unaligned, V>,
{
    type Emission = R::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::new(move |result: QueryResult<_>| match result {
            Err(original) if original.has_cause::<E>() => {
                let step = R::resolve(graphrecord, &prepared, &(), label);

                Self::Emission::map_step(step, |replacement| match replacement {
                    Ok(value) => Ok(value),
                    Err(_) => Err(original),
                })
            }
            result => Self::Emission::keep(result),
        }))
    }
}

impl<E, R> ErrorPolicy<E> for Replace<R>
where
    R: Clone + 'static,
    Self: Operation,
    E: Apply<Self>,
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(input, self.clone()))
    }
}

impl<E, D, R> ErrorPolicyOf<E, D> for Replace<R>
where
    D: Diagnostic,
    R: Clone + 'static,
    ReplaceErrorsOf<D, R>: Operation,
    E: Apply<ReplaceErrorsOf<D, R>>,
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            ReplaceErrorsOf::new(self.replacement.clone()),
        ))
    }
}

impl<E, G, R> ErrorPolicyIn<E, G> for Replace<R>
where
    G: ErrorGroup,
    R: Clone + 'static,
    ReplaceErrorsIn<G, R>: Operation,
    E: Apply<ReplaceErrorsIn<G, R>>,
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            ReplaceErrorsIn::new(self.replacement.clone()),
        ))
    }
}

impl<E, C, R> ErrorPolicyWithCause<E, C> for Replace<R>
where
    C: Error + 'static,
    R: Clone + 'static,
    ReplaceErrorsWithCause<C, R>: Operation,
    E: Apply<ReplaceErrorsWithCause<C, R>>,
{
    type Output = E::Output;

    fn build(&self, input: E) -> Self::Output {
        Self::Output::new(OperationContext::new(
            input,
            ReplaceErrorsWithCause::new(self.replacement.clone()),
        ))
    }
}

operation_manifest! {
    Replace<R> as "on_error_replace" {
        method: OnError::on_error;
        policy: Replace<R> = Replace::new(R);
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            argument: R: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            argument: R: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
