use crate::{
    Arity, AttributeName, Bare, ExpandedIndex, ExpandedIndexReference, Explain, FailureKind,
    IndexDomain, IndexValue, Indexed, Labeled, Mask, Operand, Position, Positional, QueryResult,
    Scalar, ValueType,
    element::{ElementEmission, Pipeline, Retention},
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, MaybeAbsent, MissingPolicy,
        Operation, OperationContext, Prepare, WithMissing,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::ExpandTo,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue},
};

pub type ParentResolution<'a, P, S> =
    <<S as ArgumentSource<Keyed<P>>>::Retention as ElementEmission>::Step<
        QueryResult<<<S as ExpandToSource<P>>::ParentValue as ValueType>::Value<'a>>,
    >;

pub trait ExpandToSource<P: IndexDomain>: ArgumentSource<Keyed<P>> + Clone {
    type ParentValue: ValueType;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a;
}

impl<P: IndexDomain> ExpandToSource<P> for GraphRecordValue {
    type ParentValue = Scalar;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a,
    {
        <Self as ArgumentSource<Keyed<P>>>::resolve(prepared, parent, label)
    }
}

impl<P: IndexDomain> ExpandToSource<P> for bool {
    type ParentValue = Mask;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a,
    {
        <Self as ArgumentSource<Keyed<P>>>::resolve(prepared, parent, label)
    }
}

impl<P: IndexDomain> ExpandToSource<P> for GraphRecordAttribute {
    type ParentValue = AttributeName;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a,
    {
        <Self as ArgumentSource<Keyed<P>>>::resolve(prepared, parent, label)
    }
}

impl<P: IndexDomain> ExpandToSource<P> for Position {
    type ParentValue = IndexValue<Positional>;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a,
    {
        <Self as ArgumentSource<Keyed<P>>>::resolve(prepared, parent, label)
    }
}

impl<P: IndexDomain> ExpandToSource<P> for EdgeIndex {
    type ParentValue = IndexValue<Self>;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a,
    {
        <Self as ArgumentSource<Keyed<P>>>::resolve(prepared, parent, label)
    }
}

impl<P: IndexDomain> ExpandToSource<P> for FailureKind {
    type ParentValue = IndexValue<Self>;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a,
    {
        <Self as ArgumentSource<Keyed<P>>>::resolve(prepared, parent, label)
    }
}

impl<P, I, V, A> ExpandToSource<P> for OperandHandle<Indexed<I, V>, A>
where
    P: IndexDomain,
    I: IndexDomain,
    V: ValueType,
    A: Arity,
    for<'a> Self: ArgumentSource<Keyed<P>, Value<'a> = V::Value<'a>>,
{
    type ParentValue = V;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a,
    {
        Self::resolve(prepared, parent, label)
    }
}

impl<P, V, A> ExpandToSource<P> for OperandHandle<Bare<V>, A>
where
    P: IndexDomain,
    V: ValueType,
    A: Arity,
    for<'a> Self: ArgumentSource<Keyed<P>, Value<'a> = V::Value<'a>>,
{
    type ParentValue = V;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a,
    {
        Self::resolve(prepared, parent, label)
    }
}

impl<P, S, M> ExpandToSource<P> for WithMissing<Keyed<P>, S, M>
where
    P: IndexDomain,
    S: MaybeAbsent<Keyed<P>> + ExpandToSource<P> + Clone,
    M: MissingPolicy<Keyed<P>, S>,
    for<'a> Self: ArgumentSource<Keyed<P>, Value<'a> = <S::ParentValue as ValueType>::Value<'a>>,
{
    type ParentValue = S::ParentValue;

    fn resolve_parent<'a>(
        prepared: &Self::Prepared<'a>,
        parent: &P::Index<'a>,
        label: &'static str,
    ) -> ParentResolution<'a, P, Self>
    where
        Self: 'a,
    {
        Self::resolve(prepared, parent, label)
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "ExpandTo")]
#[plan(optimizer_hints(empty = if_all))]
pub struct ExpandToOperation<S> {
    #[argument]
    parent: S,
}

impl<S: Prepare> Prepare for ExpandToOperation<S> {
    type Prepared<'a>
        = S::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.parent.prepare(graphrecord, cache)
    }
}

impl<P: IndexDomain, C: IndexDomain, W: ValueType, S: ExpandToSource<P>>
    ElementKernel<Indexed<ExpandedIndex<P, C>, W>> for ExpandToOperation<S>
{
    type Emission = S::Retention;
    type OutShape = Indexed<ExpandedIndex<P, C>, S::ParentValue>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<ExpandedIndex<P, C>, W>, Self>> {
        Ok(Pipeline::keyed(
            move |address: ExpandedIndexReference<'_, _, _>, template_outcome| {
                match template_outcome {
                    Err(failure) => <Self::Emission as Retention>::keep(Err(failure)),
                    Ok(_) => S::resolve_parent(&prepared, address.parent_index(), Self::LABEL),
                }
            },
        ))
    }
}

impl<S, T> ExpandTo<T> for S
where
    S: Clone,
    T: Apply<ExpandToOperation<S>>,
    ExpandToOperation<S>: Operation,
{
    type ReturnOperand = T::Output;

    fn expand_to(&self, template: &T) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            template.clone(),
            ExpandToOperation {
                parent: self.clone(),
            },
        ))
    }
}
