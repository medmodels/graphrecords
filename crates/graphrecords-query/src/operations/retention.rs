use crate::Arity;

pub trait Retention: 'static {
    type Step<T>;
    type OutArity<C: Arity>: Arity;
    type Or<R: Retention>: Retention;

    fn keep<T>(value: T) -> Self::Step<T>;

    fn absent<T, E>(error: impl FnOnce() -> E) -> Self::Step<Result<T, E>>;

    fn map_step<T, U>(step: Self::Step<T>, function: impl FnOnce(T) -> U) -> Self::Step<U>;

    fn collapse<T>(step: Self::Step<T>) -> Option<T>;

    fn apply<'a, C: Arity, X: 'a, Y: 'a>(
        container: C::Container<'a, X>,
        function: impl FnMut(X) -> Self::Step<Y> + 'a,
    ) -> <Self::OutArity<C> as Arity>::Container<'a, Y>;
}

pub struct Preserving;
pub struct Dropping;

impl Retention for Preserving {
    type Or<R: Retention> = R;
    type OutArity<C: Arity> = C;
    type Step<T> = T;

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

    fn apply<'a, C: Arity, X: 'a, Y: 'a>(
        container: C::Container<'a, X>,
        function: impl FnMut(X) -> Self::Step<Y> + 'a,
    ) -> <Self::OutArity<C> as Arity>::Container<'a, Y> {
        C::map_elements(container, function)
    }
}

impl Retention for Dropping {
    type Or<R: Retention> = Self;
    type OutArity<C: Arity> = C::AfterDrop;
    type Step<T> = Option<T>;

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

    fn apply<'a, C: Arity, X: 'a, Y: 'a>(
        container: C::Container<'a, X>,
        function: impl FnMut(X) -> Self::Step<Y> + 'a,
    ) -> <Self::OutArity<C> as Arity>::Container<'a, Y> {
        C::filter_map_elements(container, function)
    }
}
