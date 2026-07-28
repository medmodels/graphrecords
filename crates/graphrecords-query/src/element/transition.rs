use crate::{
    ExpandedChild, ExpandedIndex, ExpandedIndexReference, Failure, IndexDomain, QueryResult,
    ValueType,
    element::{
        Arity, Bare, ElementEmission, ElementShape, Expanding, Indexed, OrderState, Ordered,
        Retention, Unordered,
    },
    index::DuplicateExpandedChildIndex,
};
use graphrecords_utils::aliases::GrHashSet;
use std::marker::PhantomData;

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
