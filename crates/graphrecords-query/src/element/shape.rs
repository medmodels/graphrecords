use crate::{
    BareValueType, IndexDomain, QueryResult, ReturnValueType, Unit, ValueType, element::Arity,
};
use std::marker::PhantomData;

pub trait ElementShape: 'static {
    type Element<'a>: 'a;
}

pub trait ReturnShape: ElementShape {
    type ReturnElement<'a>: 'a;

    fn into_return_element(element: Self::Element<'_>) -> Self::ReturnElement<'_>;
}

pub struct Indexed<K: IndexDomain, V: ValueType>(PhantomData<(K, V)>);
pub struct Bare<V: BareValueType>(PhantomData<V>);

impl<K: IndexDomain, V: ValueType> ElementShape for Indexed<K, V> {
    type Element<'a> = (K::Index<'a>, QueryResult<V::Value<'a>>);
}
impl<V: BareValueType> ElementShape for Bare<V> {
    type Element<'a> = QueryResult<V::Value<'a>>;
}

impl<K: IndexDomain, V: ReturnValueType> ReturnShape for Indexed<K, V> {
    type ReturnElement<'a> = (K::Index<'a>, QueryResult<V::Value<'a>>);

    fn into_return_element(element: Self::Element<'_>) -> Self::ReturnElement<'_> {
        element
    }
}

impl<K: IndexDomain> ReturnShape for Indexed<K, Unit> {
    type ReturnElement<'a> = QueryResult<K::Owned>;

    fn into_return_element(element: Self::Element<'_>) -> Self::ReturnElement<'_> {
        let (index, value) = element;

        value.map(|()| K::to_owned(&index))
    }
}

impl<V: BareValueType + ReturnValueType> ReturnShape for Bare<V> {
    type ReturnElement<'a> = QueryResult<V::Value<'a>>;

    fn into_return_element(element: Self::Element<'_>) -> Self::ReturnElement<'_> {
        element
    }
}

pub type Return<'a, S, C> = <C as Arity>::Container<'a, <S as ElementShape>::Element<'a>>;
