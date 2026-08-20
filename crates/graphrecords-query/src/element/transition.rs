use crate::{
    BareValueDomain, ExpandedChild, ExpandedIndex, ExpandedIndexAddress, Failure, IndexDomain,
    QueryResult, ValueDomain,
    element::{
        Arity, Bare, ElementEmission, ElementShape, Expanding, Indexed, OrderState, Ordered,
        Retention, Unordered,
    },
    error::index::DuplicateExpandedChildIndex,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;
use std::marker::PhantomData;

type ExpandedElement<'a, P, C, V> = (
    ExpandedIndexAddress<P, C>,
    QueryResult<<V as ValueDomain>::Value<'a>>,
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

    pub(crate) fn run(&self, input: X) -> Y {
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
        <I as IndexDomain>::Address,
        QueryResult<<V as ValueDomain>::Value<'a>>,
    ),
    <E as ElementEmission>::Step<QueryResult<<W as ValueDomain>::Value<'a>>>,
    E,
>;

pub type BarePipeline<'a, V, W, E> = Pipeline<
    'a,
    QueryResult<<V as ValueDomain>::Value<'a>>,
    <E as ElementEmission>::Step<QueryResult<<W as ValueDomain>::Value<'a>>>,
    E,
>;

pub type IndexedExpansionPipeline<'a, P, C, V, W, O> = Pipeline<
    'a,
    (<P as IndexDomain>::Address, <V as ValueDomain>::Value<'a>),
    QueryResult<Vec<ExpandedChild<'a, C, W>>>,
    Expanding<O>,
>;

pub trait ElementTransition<T: ElementShape, E: ElementEmission>: ElementShape {
    type Pipeline<'a>: 'a
    where
        Self: 'a,
        T: 'a;

    fn apply<'a, C: Arity>(
        graphrecord: &'a GraphRecord,
        values: C::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <E::OutArity<C> as Arity>::Container<'a, T::Element<'a>>;
}

impl<I: IndexDomain, V: ValueDomain, W: ValueDomain, E: Retention>
    ElementTransition<Indexed<I, W>, E> for Indexed<I, V>
{
    type Pipeline<'a> = IndexedValuePipeline<'a, I, V, W, E>;

    fn apply<'a, C: Arity>(
        _graphrecord: &'a GraphRecord,
        values: C::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <E::OutArity<C> as Arity>::Container<'a, <Indexed<I, W> as ElementShape>::Element<'a>>
    {
        E::apply(values, move |element| {
            let (address, value): (I::Address, _) = element;
            let step = pipeline.run((address.clone(), value));

            <E as Retention>::map_step(step, |value| (address, value))
        })
    }
}

impl<I: IndexDomain, V: ValueDomain, W: BareValueDomain, E: ElementEmission>
    ElementTransition<Bare<W>, E> for Indexed<I, V>
{
    type Pipeline<'a> = IndexedValuePipeline<'a, I, V, W, E>;

    fn apply<'a, C: Arity>(
        _graphrecord: &'a GraphRecord,
        values: C::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <E::OutArity<C> as Arity>::Container<'a, <Bare<W> as ElementShape>::Element<'a>> {
        E::apply(values, move |value| pipeline.run(value))
    }
}

impl<V: BareValueDomain, W: BareValueDomain, E: ElementEmission> ElementTransition<Bare<W>, E>
    for Bare<V>
{
    type Pipeline<'a> = BarePipeline<'a, V, W, E>;

    fn apply<'a, C: Arity>(
        _graphrecord: &'a GraphRecord,
        values: C::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <E::OutArity<C> as Arity>::Container<'a, <Bare<W> as ElementShape>::Element<'a>> {
        E::apply(values, move |value| pipeline.run(value))
    }
}

fn expand_indexed_source<'a, P, C, V, W, O>(
    graphrecord: &'a GraphRecord,
    parent: P::Address,
    source: QueryResult<V::Value<'a>>,
    pipeline: &IndexedExpansionPipeline<'a, P, C, V, W, O>,
) -> Vec<ExpandedElement<'a, P, C, W>>
where
    P: IndexDomain,
    C: IndexDomain,
    V: ValueDomain,
    W: ValueDomain,
    O: OrderState,
    Expanding<O>: ElementEmission,
{
    let source_value = match source {
        Ok(value) => value,
        Err(failure) => {
            return vec![(ExpandedIndexAddress::parent(parent), Err(failure))];
        }
    };

    let children = match pipeline.run((parent.clone(), source_value)) {
        Ok(children) => children,
        Err(failure) => {
            return vec![(ExpandedIndexAddress::parent(parent), Err(failure))];
        }
    };

    let mut seen_children = GrHashSet::default();
    let mut fragment = Vec::with_capacity(children.len());

    for child in children {
        let (child_address, outcome) = child.into_parts();

        if !seen_children.insert(child_address.clone()) {
            let parent_address = ExpandedIndexAddress::parent(parent);
            let failure = Failure::new_at_address::<ExpandedIndex<P, C>, _>(
                DuplicateExpandedChildIndex::<C>::new(C::own_index(&C::index(
                    graphrecord,
                    &child_address,
                ))),
                graphrecord,
                &parent_address,
                "indexed expansion",
            );

            return vec![(parent_address, Err(failure))];
        }

        fragment.push((
            ExpandedIndexAddress::child(parent.clone(), child_address),
            outcome,
        ));
    }

    fragment
}

impl<P: IndexDomain, C: IndexDomain, V: ValueDomain, W: ValueDomain>
    ElementTransition<Indexed<ExpandedIndex<P, C>, W>, Expanding<Ordered>> for Indexed<P, V>
{
    type Pipeline<'a>
        = IndexedExpansionPipeline<'a, P, C, V, W, Ordered>
    where
        Self: 'a,
        Indexed<ExpandedIndex<P, C>, W>: 'a;

    fn apply<'a, A: Arity>(
        graphrecord: &'a GraphRecord,
        values: A::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <<Expanding<Ordered> as ElementEmission>::OutArity<A> as Arity>::Container<
        'a,
        <Indexed<ExpandedIndex<P, C>, W> as ElementShape>::Element<'a>,
    > {
        Expanding::<Ordered>::apply::<A, _, _>(values, move |(parent, source)| {
            expand_indexed_source::<_, _, V, _, _>(graphrecord, parent, source, &pipeline)
        })
    }
}

impl<P: IndexDomain, C: IndexDomain, V: ValueDomain, W: ValueDomain>
    ElementTransition<Indexed<ExpandedIndex<P, C>, W>, Expanding<Unordered>> for Indexed<P, V>
{
    type Pipeline<'a>
        = IndexedExpansionPipeline<'a, P, C, V, W, Unordered>
    where
        Self: 'a,
        Indexed<ExpandedIndex<P, C>, W>: 'a;

    fn apply<'a, A: Arity>(
        graphrecord: &'a GraphRecord,
        values: A::Container<'a, Self::Element<'a>>,
        pipeline: Self::Pipeline<'a>,
    ) -> <<Expanding<Unordered> as ElementEmission>::OutArity<A> as Arity>::Container<
        'a,
        <Indexed<ExpandedIndex<P, C>, W> as ElementShape>::Element<'a>,
    > {
        Expanding::<Unordered>::apply::<A, _, _>(values, move |(parent, source)| {
            expand_indexed_source::<_, _, V, _, _>(graphrecord, parent, source, &pipeline)
        })
    }
}
