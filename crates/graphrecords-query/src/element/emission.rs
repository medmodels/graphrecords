use crate::{
    element::{Arity, OrderState, Ordered, Unordered},
    optimizer::Estimate,
    sealed::Sealed,
};
use std::marker::PhantomData;

pub trait ElementEmission: Sealed + 'static {
    type Step<T>;
    type OutArity<C: Arity>: Arity;

    fn map_step<T, U>(step: Self::Step<T>, function: impl Fn(T) -> U) -> Self::Step<U>;

    fn apply<'a, C: Arity, X: 'a, Y: 'a>(
        container: C::Container<'a, X>,
        function: impl Fn(X) -> Self::Step<Y> + 'a,
    ) -> <Self::OutArity<C> as Arity>::Container<'a, Y>;

    fn default_estimate(input: Estimate) -> Estimate;
}

pub struct Preserving;
pub struct Dropping;
pub struct Expanding<O: OrderState>(PhantomData<O>);

impl Sealed for Preserving {}
impl Sealed for Dropping {}
impl<O: OrderState> Sealed for Expanding<O> {}

impl ElementEmission for Preserving {
    type OutArity<C: Arity> = C;
    type Step<T> = T;

    fn map_step<T, U>(step: Self::Step<T>, function: impl Fn(T) -> U) -> Self::Step<U> {
        function(step)
    }

    fn apply<'a, C: Arity, X: 'a, Y: 'a>(
        container: C::Container<'a, X>,
        function: impl Fn(X) -> Self::Step<Y> + 'a,
    ) -> <Self::OutArity<C> as Arity>::Container<'a, Y> {
        C::map_elements(container, function)
    }

    fn default_estimate(input: Estimate) -> Estimate {
        Estimate {
            elements: input.elements,
            distinct: None,
            selectivity: None,
            per_group: None,
        }
    }
}

impl ElementEmission for Dropping {
    type OutArity<C: Arity> = C::AfterDrop;
    type Step<T> = Option<T>;

    fn map_step<T, U>(step: Self::Step<T>, function: impl Fn(T) -> U) -> Self::Step<U> {
        step.map(function)
    }

    fn apply<'a, C: Arity, X: 'a, Y: 'a>(
        container: C::Container<'a, X>,
        function: impl Fn(X) -> Self::Step<Y> + 'a,
    ) -> <Self::OutArity<C> as Arity>::Container<'a, Y> {
        C::filter_map_elements(container, function)
    }

    fn default_estimate(_input: Estimate) -> Estimate {
        Estimate::UNKNOWN
    }
}

impl ElementEmission for Expanding<Ordered> {
    type OutArity<C: Arity> = C::AfterOrderedExpansion;
    type Step<T> = Vec<T>;

    fn map_step<T, U>(step: Self::Step<T>, function: impl Fn(T) -> U) -> Self::Step<U> {
        step.into_iter().map(function).collect()
    }

    fn apply<'a, C: Arity, X: 'a, Y: 'a>(
        container: C::Container<'a, X>,
        function: impl Fn(X) -> Self::Step<Y> + 'a,
    ) -> <Self::OutArity<C> as Arity>::Container<'a, Y> {
        C::flat_map_ordered_elements(container, function)
    }

    fn default_estimate(_input: Estimate) -> Estimate {
        Estimate::UNKNOWN
    }
}

impl ElementEmission for Expanding<Unordered> {
    type OutArity<C: Arity> = C::AfterUnorderedExpansion;
    type Step<T> = Vec<T>;

    fn map_step<T, U>(step: Self::Step<T>, function: impl Fn(T) -> U) -> Self::Step<U> {
        step.into_iter().map(function).collect()
    }

    fn apply<'a, C: Arity, X: 'a, Y: 'a>(
        container: C::Container<'a, X>,
        function: impl Fn(X) -> Self::Step<Y> + 'a,
    ) -> <Self::OutArity<C> as Arity>::Container<'a, Y> {
        C::flat_map_unordered_elements(container, function)
    }

    fn default_estimate(_input: Estimate) -> Estimate {
        Estimate::UNKNOWN
    }
}

pub trait Retention: ElementEmission {
    type Or<R: Retention>: Retention;

    fn keep<T>(value: T) -> Self::Step<T>;

    fn absent<T, E>(error: impl FnOnce() -> E) -> Self::Step<Result<T, E>>;

    fn map_step<T, U>(step: Self::Step<T>, function: impl FnOnce(T) -> U) -> Self::Step<U>;

    fn collapse<T>(step: Self::Step<T>) -> Option<T>;
}

impl Retention for Preserving {
    type Or<R: Retention> = R;

    fn keep<T>(value: T) -> Self::Step<T> {
        value
    }

    fn absent<T, E>(error: impl FnOnce() -> E) -> Self::Step<Result<T, E>> {
        Err(error())
    }

    fn map_step<T, U>(step: Self::Step<T>, function: impl FnOnce(T) -> U) -> Self::Step<U> {
        function(step)
    }

    fn collapse<T>(step: Self::Step<T>) -> Option<T> {
        Some(step)
    }
}

impl Retention for Dropping {
    type Or<R: Retention> = Self;

    fn keep<T>(value: T) -> Self::Step<T> {
        Some(value)
    }

    fn absent<T, E>(_error: impl FnOnce() -> E) -> Self::Step<Result<T, E>> {
        None
    }

    fn map_step<T, U>(step: Self::Step<T>, function: impl FnOnce(T) -> U) -> Self::Step<U> {
        step.map(function)
    }

    fn collapse<T>(step: Self::Step<T>) -> Option<T> {
        step
    }
}
