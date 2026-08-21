use crate::{
    BareValueDomain, IndexDomain, QueryResult, ReturnValueDomain, Unit, ValueDomain, element::Arity,
};
use graphrecords_core::GraphRecord;
use std::marker::PhantomData;

pub trait ElementShape: 'static {
    type Element<'a>: 'a;

    type ValueDomain: ValueDomain;
}

pub trait ReturnShape: ElementShape {
    type ReturnElement<'a>: 'a;

    fn into_return_element<'a>(
        graphrecord: &'a GraphRecord,
        element: Self::Element<'a>,
    ) -> Self::ReturnElement<'a>;
}

pub struct Indexed<I: IndexDomain, V: ValueDomain>(PhantomData<(I, V)>);
pub struct Bare<V: BareValueDomain>(PhantomData<V>);

impl<I: IndexDomain, V: ValueDomain> ElementShape for Indexed<I, V> {
    type Element<'a> = (I::Address, QueryResult<V::Value<'a>>);
    type ValueDomain = V;
}

impl<V: BareValueDomain> ElementShape for Bare<V> {
    type Element<'a> = QueryResult<V::Value<'a>>;
    type ValueDomain = V;
}

impl<I: IndexDomain, V: ReturnValueDomain> ReturnShape for Indexed<I, V> {
    type ReturnElement<'a> = (I::Index<'a>, QueryResult<V::Value<'a>>);

    fn into_return_element<'a>(
        graphrecord: &'a GraphRecord,
        element: Self::Element<'a>,
    ) -> Self::ReturnElement<'a> {
        let (address, value) = element;

        (I::index(graphrecord, &address), value)
    }
}

impl<I: IndexDomain> ReturnShape for Indexed<I, Unit> {
    type ReturnElement<'a> = QueryResult<I::Owned>;

    fn into_return_element<'a>(
        graphrecord: &'a GraphRecord,
        element: Self::Element<'a>,
    ) -> Self::ReturnElement<'a> {
        let (address, value) = element;

        value.map(|()| I::own_index(&I::index(graphrecord, &address)))
    }
}

impl<V: BareValueDomain + ReturnValueDomain> ReturnShape for Bare<V> {
    type ReturnElement<'a> = QueryResult<V::Value<'a>>;

    fn into_return_element<'a>(
        _graphrecord: &'a GraphRecord,
        element: Self::Element<'a>,
    ) -> Self::ReturnElement<'a> {
        element
    }
}

pub type Return<'a, S, C> = <C as Arity>::Container<'a, <S as ElementShape>::Element<'a>>;
