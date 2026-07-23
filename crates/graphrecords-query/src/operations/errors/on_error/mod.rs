mod drop;
mod raise;
mod replace;

use crate::{
    Diagnostic, ErrorGroup, Operand,
    traits::{OnError, OnErrorIn, OnErrorOf, OnErrorWithCause},
};
pub use drop::Drop;
pub use raise::{Raise, RaiseWhen};
pub use replace::Replace;
use std::error::Error;

pub trait ErrorPolicy<I: Operand>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

pub trait ErrorPolicyOf<I: Operand, D: Diagnostic>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

pub trait ErrorPolicyIn<I: Operand, G: ErrorGroup>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

pub trait ErrorPolicyWithCause<I: Operand, C: Error + 'static>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

impl<O: Operand> OnError for O {
    fn on_error<A: ErrorPolicy<Self>>(&self, policy: A) -> A::Output {
        <A as ErrorPolicy<Self>>::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnErrorOf<A> for O {
    type Output<D: Diagnostic>
        = <A as ErrorPolicyOf<Self, D>>::Output
    where
        A: ErrorPolicyOf<Self, D>;

    fn on_error_of<D: Diagnostic>(&self, policy: A) -> Self::Output<D>
    where
        A: ErrorPolicyOf<Self, D>,
    {
        <A as ErrorPolicyOf<Self, D>>::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnErrorIn<A> for O {
    type Output<G: ErrorGroup>
        = <A as ErrorPolicyIn<Self, G>>::Output
    where
        A: ErrorPolicyIn<Self, G>;

    fn on_error_in<G: ErrorGroup>(&self, policy: A) -> Self::Output<G>
    where
        A: ErrorPolicyIn<Self, G>,
    {
        <A as ErrorPolicyIn<Self, G>>::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnErrorWithCause<A> for O {
    type Output<C: Error + 'static>
        = <A as ErrorPolicyWithCause<Self, C>>::Output
    where
        A: ErrorPolicyWithCause<Self, C>;

    fn on_error_with_cause<C: Error + 'static>(&self, policy: A) -> Self::Output<C>
    where
        A: ErrorPolicyWithCause<Self, C>,
    {
        <A as ErrorPolicyWithCause<Self, C>>::build(&policy, self.clone())
    }
}
