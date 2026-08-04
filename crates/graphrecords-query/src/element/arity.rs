use crate::sealed::Sealed;
use std::marker::PhantomData;

pub type BoxedIterator<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

pub trait Arity: 'static {
    type Container<'a, X: 'a>: 'a;
    type AfterDrop: Arity;
    type AfterOrderedExpansion: Arity;
    type AfterUnorderedExpansion: Arity;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y>;

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y>;

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y>;

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y>;
}

pub trait OrderState: Sealed + 'static {}

pub struct Ordered;
pub struct Unordered;

impl Sealed for Ordered {}
impl Sealed for Unordered {}

impl OrderState for Ordered {}

impl OrderState for Unordered {}

pub struct Multiple<O: OrderState>(PhantomData<O>);
pub struct Single;
pub struct Definite;

impl<O: OrderState> Arity for Multiple<O> {
    type AfterDrop = Self;
    type AfterOrderedExpansion = Self;
    type AfterUnorderedExpansion = Multiple<Unordered>;
    type Container<'a, X: 'a> = BoxedIterator<'a, X>;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        Box::new(container.map(function))
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        Box::new(container.filter_map(function))
    }

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y> {
        Box::new(container.flat_map(function))
    }

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y> {
        Box::new(container.flat_map(function))
    }
}

impl Arity for Single {
    type AfterDrop = Self;
    type AfterOrderedExpansion = Multiple<Ordered>;
    type AfterUnorderedExpansion = Multiple<Unordered>;
    type Container<'a, X: 'a> = Option<X>;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        container.map(function)
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        container.and_then(function)
    }

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y> {
        Box::new(container.into_iter().flat_map(function))
    }

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y> {
        Box::new(container.into_iter().flat_map(function))
    }
}

impl Arity for Definite {
    type AfterDrop = Single;
    type AfterOrderedExpansion = Multiple<Ordered>;
    type AfterUnorderedExpansion = Multiple<Unordered>;
    type Container<'a, X: 'a> = X;

    fn map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Y + 'a,
    ) -> Self::Container<'a, Y> {
        function(container)
    }

    fn filter_map_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Option<Y> + 'a,
    ) -> <Self::AfterDrop as Arity>::Container<'a, Y> {
        function(container)
    }

    fn flat_map_ordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterOrderedExpansion as Arity>::Container<'a, Y> {
        Box::new(function(container).into_iter())
    }

    fn flat_map_unordered_elements<'a, X: 'a, Y: 'a>(
        container: Self::Container<'a, X>,
        function: impl Fn(X) -> Vec<Y> + 'a,
    ) -> <Self::AfterUnorderedExpansion as Arity>::Container<'a, Y> {
        Box::new(function(container).into_iter())
    }
}
