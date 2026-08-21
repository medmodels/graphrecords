use super::{DynIndex, DynIndexAddress, DynValue, DynValueView};
use crate::{
    Arity, Bare, BoxedIterator, Definite, ElementShape, Indexed, Mask, Multiple, Ordered,
    QueryResult, Single, Unit, Unordered,
};

pub enum DynArityStream<'a, T: 'a> {
    MultipleOrdered(BoxedIterator<'a, T>),
    MultipleUnordered(BoxedIterator<'a, T>),
    Single(Option<T>),
    Definite(T),
}

pub enum DynStream<'a> {
    IndexedValue(DynArityStream<'a, (DynIndexAddress, QueryResult<DynValueView<'a>>)>),
    IndexedMask(DynArityStream<'a, (DynIndexAddress, QueryResult<bool>)>),
    IndexedUnit(DynArityStream<'a, (DynIndexAddress, QueryResult<()>)>),
    BareValue(DynArityStream<'a, QueryResult<DynValueView<'a>>>),
    BareMask(DynArityStream<'a, QueryResult<bool>>),
}

pub trait DynArity: Arity {
    fn erase<'a, T: 'a>(container: Self::Container<'a, T>) -> DynArityStream<'a, T>;

    fn project<'a, T: 'a>(stream: DynArityStream<'a, T>) -> Self::Container<'a, T>;
}

impl DynArity for Multiple<Ordered> {
    fn erase<'a, T: 'a>(container: Self::Container<'a, T>) -> DynArityStream<'a, T> {
        DynArityStream::MultipleOrdered(container)
    }

    fn project<'a, T: 'a>(stream: DynArityStream<'a, T>) -> Self::Container<'a, T> {
        let DynArityStream::MultipleOrdered(container) = stream else {
            panic!("registry selected an ordered-multiple lane for a different dynamic arity")
        };
        container
    }
}

impl DynArity for Multiple<Unordered> {
    fn erase<'a, T: 'a>(container: Self::Container<'a, T>) -> DynArityStream<'a, T> {
        DynArityStream::MultipleUnordered(container)
    }

    fn project<'a, T: 'a>(stream: DynArityStream<'a, T>) -> Self::Container<'a, T> {
        let DynArityStream::MultipleUnordered(container) = stream else {
            panic!("registry selected an unordered-multiple lane for a different dynamic arity")
        };
        container
    }
}

impl DynArity for Single {
    fn erase<'a, T: 'a>(container: Self::Container<'a, T>) -> DynArityStream<'a, T> {
        DynArityStream::Single(container)
    }

    fn project<'a, T: 'a>(stream: DynArityStream<'a, T>) -> Self::Container<'a, T> {
        let DynArityStream::Single(container) = stream else {
            panic!("registry selected a single lane for a different dynamic arity")
        };
        container
    }
}

impl DynArity for Definite {
    fn erase<'a, T: 'a>(container: Self::Container<'a, T>) -> DynArityStream<'a, T> {
        DynArityStream::Definite(container)
    }

    fn project<'a, T: 'a>(stream: DynArityStream<'a, T>) -> Self::Container<'a, T> {
        let DynArityStream::Definite(container) = stream else {
            panic!("registry selected a definite lane for a different dynamic arity")
        };
        container
    }
}

pub trait DynStreamShape: ElementShape {
    fn erase<'a, C: DynArity>(container: C::Container<'a, Self::Element<'a>>) -> DynStream<'a>
    where
        Self: 'a;

    fn project<'a, C: DynArity>(stream: DynStream<'a>) -> C::Container<'a, Self::Element<'a>>
    where
        Self: 'a;
}

impl DynStreamShape for Indexed<DynIndex, DynValue> {
    fn erase<'a, C: DynArity>(container: C::Container<'a, Self::Element<'a>>) -> DynStream<'a>
    where
        Self: 'a,
    {
        DynStream::IndexedValue(C::erase(container))
    }

    fn project<'a, C: DynArity>(stream: DynStream<'a>) -> C::Container<'a, Self::Element<'a>>
    where
        Self: 'a,
    {
        let DynStream::IndexedValue(stream) = stream else {
            panic!("registry selected an indexed dynamic-value lane for a different dynamic shape")
        };
        C::project(stream)
    }
}

impl DynStreamShape for Indexed<DynIndex, Mask> {
    fn erase<'a, C: DynArity>(container: C::Container<'a, Self::Element<'a>>) -> DynStream<'a>
    where
        Self: 'a,
    {
        DynStream::IndexedMask(C::erase(container))
    }

    fn project<'a, C: DynArity>(stream: DynStream<'a>) -> C::Container<'a, Self::Element<'a>>
    where
        Self: 'a,
    {
        let DynStream::IndexedMask(stream) = stream else {
            panic!("registry selected an indexed mask lane for a different dynamic shape")
        };
        C::project(stream)
    }
}

impl DynStreamShape for Indexed<DynIndex, Unit> {
    fn erase<'a, C: DynArity>(container: C::Container<'a, Self::Element<'a>>) -> DynStream<'a>
    where
        Self: 'a,
    {
        DynStream::IndexedUnit(C::erase(container))
    }

    fn project<'a, C: DynArity>(stream: DynStream<'a>) -> C::Container<'a, Self::Element<'a>>
    where
        Self: 'a,
    {
        let DynStream::IndexedUnit(stream) = stream else {
            panic!("registry selected an indexed unit lane for a different dynamic shape")
        };
        C::project(stream)
    }
}

impl DynStreamShape for Bare<DynValue> {
    fn erase<'a, C: DynArity>(container: C::Container<'a, Self::Element<'a>>) -> DynStream<'a>
    where
        Self: 'a,
    {
        DynStream::BareValue(C::erase(container))
    }

    fn project<'a, C: DynArity>(stream: DynStream<'a>) -> C::Container<'a, Self::Element<'a>>
    where
        Self: 'a,
    {
        let DynStream::BareValue(stream) = stream else {
            panic!("registry selected a bare dynamic-value lane for a different dynamic shape")
        };
        C::project(stream)
    }
}

impl DynStreamShape for Bare<Mask> {
    fn erase<'a, C: DynArity>(container: C::Container<'a, Self::Element<'a>>) -> DynStream<'a>
    where
        Self: 'a,
    {
        DynStream::BareMask(C::erase(container))
    }

    fn project<'a, C: DynArity>(stream: DynStream<'a>) -> C::Container<'a, Self::Element<'a>>
    where
        Self: 'a,
    {
        let DynStream::BareMask(stream) = stream else {
            panic!("registry selected a bare mask lane for a different dynamic shape")
        };
        C::project(stream)
    }
}
