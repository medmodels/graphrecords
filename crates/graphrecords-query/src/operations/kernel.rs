use crate::{
    Arity, Bare, ElementShape, EvaluateOperand, ExpandedChild, ExpandedIndex,
    ExpandedIndexReference, Failure, IndexDomain, Indexed, Operand, OrderState, Ordered,
    QueryResult, Unordered, ValueType,
    operands::{DuplicateExpandedChildIndex, GroupOperand, OperandHandle, Partition},
    operations::{
        Apply, Element, ElementEmission, Expanding, Group, GroupKey, Lane, Operation,
        OperationScope, Retention,
    },
    optimizer::{Estimate, Stats},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;
use std::marker::PhantomData;

pub type KeyedStream<'a, I, V, C> =
    <OperandHandle<Indexed<I, V>, C> as EvaluateOperand>::ReturnValue<'a>;

pub type BareStream<'a, V, C> = <OperandHandle<Bare<V>, C> as EvaluateOperand>::ReturnValue<'a>;

type ExpandedElement<'a, P, C, V> = (
    ExpandedIndexReference<'a, P, C>,
    QueryResult<<V as ValueType>::Value<'a>>,
);

pub struct Pipeline<'a, X: 'a, Y: 'a, E: ElementEmission> {
    run: Box<dyn Fn(X) -> Y + 'a>,
    emission: PhantomData<fn() -> E>,
}

impl<'a, X: 'a, Y: 'a, E: ElementEmission> Pipeline<'a, X, Y, E> {
    #[must_use]
    pub fn new(run: impl Fn(X) -> Y + 'a) -> Self {
        Self {
            run: Box::new(run),
            emission: PhantomData,
        }
    }

    fn run(&self, input: X) -> Y {
        (self.run)(input)
    }
}

impl<'a, A: 'a, B: 'a, Y: 'a, E: ElementEmission> Pipeline<'a, (A, B), Y, E> {
    #[must_use]
    pub fn keyed(run: impl Fn(A, B) -> Y + 'a) -> Self {
        Self::new(move |(first, second)| run(first, second))
    }

    #[must_use]
    pub fn unkeyed(run: impl Fn(B) -> Y + 'a) -> Self {
        Self::new(move |(_, second)| run(second))
    }
}

pub type IndexedValuePipeline<'a, I, V, W, E> = Pipeline<
    'a,
    (
        <I as IndexDomain>::Index<'a>,
        QueryResult<<V as ValueType>::Value<'a>>,
    ),
    <E as ElementEmission>::Step<QueryResult<<W as ValueType>::Value<'a>>>,
    E,
>;

pub type IndexedToBarePipeline<'a, I, V, W, E> = IndexedValuePipeline<'a, I, V, W, E>;

pub type BarePipeline<'a, V, W, E> = Pipeline<
    'a,
    QueryResult<<V as ValueType>::Value<'a>>,
    <E as ElementEmission>::Step<QueryResult<<W as ValueType>::Value<'a>>>,
    E,
>;

pub type IndexedExpansionPipeline<'a, P, C, V, W, O> = Pipeline<
    'a,
    (<P as IndexDomain>::Index<'a>, <V as ValueType>::Value<'a>),
    QueryResult<Vec<ExpandedChild<'a, C, W>>>,
    Expanding<O>,
>;

pub trait ElementTransition<T: ElementShape, E: ElementEmission>: ElementShape {
    type Pipeline<'a>: 'a
    where
        Self: 'a,
        T: 'a;

    fn apply<'a, C: Arity>(
        values: C::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <E::OutArity<C> as Arity>::Container<'a, T::Element<'a>>;
}

impl<I: IndexDomain, V: ValueType, W: ValueType, E: Retention> ElementTransition<Indexed<I, W>, E>
    for Indexed<I, V>
{
    type Pipeline<'a> = IndexedValuePipeline<'a, I, V, W, E>;

    fn apply<'a, C: Arity>(
        values: C::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <E::OutArity<C> as Arity>::Container<'a, <Indexed<I, W> as ElementShape>::Element<'a>>
    {
        E::apply(values, move |element| {
            let (index, value): (I::Index<'a>, _) = element;
            let step = pipeline.run((index.clone(), value));

            <E as Retention>::map_step(step, |value| (index, value))
        })
    }
}

impl<I: IndexDomain, V: ValueType, W: ValueType, E: ElementEmission> ElementTransition<Bare<W>, E>
    for Indexed<I, V>
{
    type Pipeline<'a> = IndexedToBarePipeline<'a, I, V, W, E>;

    fn apply<'a, C: Arity>(
        values: C::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <E::OutArity<C> as Arity>::Container<'a, <Bare<W> as ElementShape>::Element<'a>> {
        E::apply(values, move |value| pipeline.run(value))
    }
}

impl<V: ValueType, W: ValueType, E: ElementEmission> ElementTransition<Bare<W>, E> for Bare<V> {
    type Pipeline<'a> = BarePipeline<'a, V, W, E>;

    fn apply<'a, C: Arity>(
        values: C::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <E::OutArity<C> as Arity>::Container<'a, <Bare<W> as ElementShape>::Element<'a>> {
        E::apply(values, move |value| pipeline.run(value))
    }
}

fn expand_indexed_source<'a, P, C, V, W, O>(
    parent: P::Index<'a>,
    source: QueryResult<V::Value<'a>>,
    pipeline: &IndexedExpansionPipeline<'a, P, C, V, W, O>,
) -> Vec<ExpandedElement<'a, P, C, W>>
where
    P: IndexDomain,
    C: IndexDomain,
    V: ValueType,
    W: ValueType,
    O: OrderState,
    Expanding<O>: ElementEmission,
{
    let source_value = match source {
        Ok(value) => value,
        Err(failure) => {
            return vec![(ExpandedIndexReference::source(parent), Err(failure))];
        }
    };

    let children = match pipeline.run((parent.clone(), source_value)) {
        Ok(children) => children,
        Err(failure) => {
            return vec![(ExpandedIndexReference::source(parent), Err(failure))];
        }
    };

    let mut seen_children = GrHashSet::default();
    let mut fragment = Vec::with_capacity(children.len());

    for child in children {
        if !seen_children.insert(C::to_owned(&child.index)) {
            let source_address = ExpandedIndexReference::source(parent.clone());
            let failure = Failure::new_at::<ExpandedIndex<_, _>, _>(
                "indexed expansion",
                DuplicateExpandedChildIndex::<C>::new(C::to_owned(&child.index)),
                &source_address,
            );

            return vec![(source_address, Err(failure))];
        }

        fragment.push((
            ExpandedIndexReference::child(parent.clone(), child.index),
            child.outcome,
        ));
    }

    fragment
}

impl<P: IndexDomain, C: IndexDomain, V: ValueType, W: ValueType>
    ElementTransition<Indexed<ExpandedIndex<P, C>, W>, Expanding<Ordered>> for Indexed<P, V>
{
    type Pipeline<'a>
        = IndexedExpansionPipeline<'a, P, C, V, W, Ordered>
    where
        Self: 'a,
        Indexed<ExpandedIndex<P, C>, W>: 'a;

    fn apply<'a, A: Arity>(
        values: A::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <<Expanding<Ordered> as ElementEmission>::OutArity<A> as Arity>::Container<
        'a,
        <Indexed<ExpandedIndex<P, C>, W> as ElementShape>::Element<'a>,
    > {
        <Expanding<Ordered> as ElementEmission>::apply::<A, _, _>(
            values,
            move |(parent, source)| {
                expand_indexed_source::<_, _, V, _, _>(parent, source, &pipeline)
            },
        )
    }
}

impl<P: IndexDomain, C: IndexDomain, V: ValueType, W: ValueType>
    ElementTransition<Indexed<ExpandedIndex<P, C>, W>, Expanding<Unordered>> for Indexed<P, V>
{
    type Pipeline<'a>
        = IndexedExpansionPipeline<'a, P, C, V, W, Unordered>
    where
        Self: 'a,
        Indexed<ExpandedIndex<P, C>, W>: 'a;

    fn apply<'a, A: Arity>(
        values: A::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <<Expanding<Unordered> as ElementEmission>::OutArity<A> as Arity>::Container<
        'a,
        <Indexed<ExpandedIndex<P, C>, W> as ElementShape>::Element<'a>,
    > {
        <Expanding<Unordered> as ElementEmission>::apply::<A, _, _>(
            values,
            move |(parent, source)| {
                expand_indexed_source::<_, _, V, _, _>(parent, source, &pipeline)
            },
        )
    }
}

pub type ElementPipeline<'a, S, P> = <S as ElementTransition<
    <P as ElementKernel<S>>::OutShape,
    <P as ElementKernel<S>>::Emission,
>>::Pipeline<'a>;

pub trait ElementKernel<S: ElementShape + ElementTransition<Self::OutShape, Self::Emission>>:
    Operation<Scope = Element>
{
    type OutShape: ElementShape;
    type Emission: ElementEmission;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, S, Self>>;

    #[allow(unused_variables)]
    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        Self::Emission::default_estimate(input)
    }
}

pub trait LaneKernel<S: ElementShape, C: Arity>: Operation<Scope = Lane> {
    type Output: Operand;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: <OperandHandle<S, C> as EvaluateOperand>::ReturnValue<'a>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>;

    #[allow(unused_variables)]
    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        Estimate::UNKNOWN
    }
}

pub trait GroupKernel<M: IndexDomain, K: GroupKey, O: Operand>: Operation<Scope = Group> {
    type Output: Operand;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, O>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>;

    #[allow(unused_variables)]
    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        Estimate::UNKNOWN
    }
}

impl<S, C, P> Apply<P, Element> for OperandHandle<S, C>
where
    S: ElementShape + ElementTransition<P::OutShape, P::Emission>,
    C: Arity,
    P: ElementKernel<S>,
{
    type Output = OperandHandle<P::OutShape, <P::Emission as ElementEmission>::OutArity<C>>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let pipeline = P::pipeline(graphrecord, prepared)?;

        Ok(S::apply(values, pipeline))
    }

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        ElementKernel::estimate(operation, input, stats)
    }
}

impl<S: ElementShape, C: Arity, P: LaneKernel<S, C>> Apply<P, Lane> for OperandHandle<S, C> {
    type Output = P::Output;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        P::execute(graphrecord, values, prepared)
    }

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        LaneKernel::estimate(operation, input, stats)
    }
}

impl<M, K, S, C, P> Apply<P, Element> for GroupOperand<M, K, OperandHandle<S, C>>
where
    M: IndexDomain,
    K: GroupKey,
    S: ElementShape + ElementTransition<P::OutShape, P::Emission>,
    C: Arity,
    P: ElementKernel<S>,
{
    type Output = GroupOperand<M, K, <OperandHandle<S, C> as Apply<P, Element>>::Output>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(values.map_payloads(|_, _, payload| {
            payload.and_then(|values| {
                <OperandHandle<S, C> as Apply<P, Element>>::apply(
                    graphrecord,
                    values,
                    prepared.clone(),
                )
            })
        }))
    }

    fn estimate(operation: &P, mut input: Estimate, stats: &Stats) -> Estimate {
        input.per_group = input.per_group.map(|estimate| {
            Box::new(<OperandHandle<S, C> as Apply<P, Element>>::estimate(
                operation, *estimate, stats,
            ))
        });
        input
    }
}

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: Arity, P: LaneKernel<S, C>> Apply<P, Lane>
    for GroupOperand<M, K, OperandHandle<S, C>>
{
    type Output = GroupOperand<M, K, <OperandHandle<S, C> as Apply<P, Lane>>::Output>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(values.map_payloads(|_, _, payload| {
            payload.and_then(|values| {
                <OperandHandle<S, C> as Apply<P, Lane>>::apply(
                    graphrecord,
                    values,
                    prepared.clone(),
                )
            })
        }))
    }

    fn estimate(operation: &P, mut input: Estimate, stats: &Stats) -> Estimate {
        input.per_group = input.per_group.map(|estimate| {
            Box::new(<OperandHandle<S, C> as Apply<P, Lane>>::estimate(
                operation, *estimate, stats,
            ))
        });
        input
    }
}

impl<M, K, S, C, P> Apply<P, Group> for GroupOperand<M, K, OperandHandle<S, C>>
where
    M: IndexDomain,
    K: GroupKey,
    S: ElementShape,
    C: Arity,
    P: GroupKernel<M, K, OperandHandle<S, C>>,
{
    type Output = P::Output;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        P::execute(graphrecord, values, prepared)
    }

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        GroupKernel::estimate(operation, input, stats)
    }
}

impl<M, K, N, L, O, P, S> Apply<P, S> for GroupOperand<M, K, GroupOperand<N, L, O>>
where
    M: IndexDomain,
    K: GroupKey,
    N: IndexDomain,
    L: GroupKey,
    O: Operand,
    P: Operation<Scope = S>,
    S: OperationScope,
    GroupOperand<N, L, O>: Apply<P, S>,
{
    type Output = GroupOperand<M, K, <GroupOperand<N, L, O> as Apply<P, S>>::Output>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(values.map_payloads(|_, _, payload| {
            payload.and_then(|values| {
                <GroupOperand<N, L, O> as Apply<P, S>>::apply(graphrecord, values, prepared.clone())
            })
        }))
    }

    fn estimate(operation: &P, mut input: Estimate, stats: &Stats) -> Estimate {
        input.per_group = input.per_group.map(|estimate| {
            Box::new(<GroupOperand<N, L, O> as Apply<P, S>>::estimate(
                operation, *estimate, stats,
            ))
        });
        input
    }
}
