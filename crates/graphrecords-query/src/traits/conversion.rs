use crate::{
    Expression, ValueDomain,
    cast::CastTarget,
    operations::{Apply, TransitionOperation},
};

pub trait Cast<T: CastTarget> {
    type Output;

    fn cast(&self, target: T) -> Self::Output;
}

pub trait DiscardIndex {
    type Output;

    fn discard_index(&self) -> Self::Output;
}

pub trait DiscardValue {
    type Output;

    fn discard_value(&self) -> Self::Output;
}

pub trait Enumerate {
    type Output;

    fn enumerate(&self) -> Self::Output;
}

pub trait Inherit<S> {
    type Output;

    fn inherit(&self, values: S) -> Self::Output;
}

pub trait Transition {
    type Expression: Expression;

    type Output<T>
    where
        T: ValueDomain,
        Self::Expression: Apply<TransitionOperation<T>>;

    fn transition<T>(&self) -> Self::Output<T>
    where
        T: ValueDomain,
        Self::Expression: Apply<TransitionOperation<T>>;
}
