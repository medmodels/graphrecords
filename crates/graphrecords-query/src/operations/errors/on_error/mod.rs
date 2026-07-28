mod drop;
mod raise;
mod replace;

use crate::{
    Diagnostic, ErrorGroup, Operand,
    traits::{OnError, OnErrorIn, OnErrorOf, OnErrorWithCause},
};
pub use drop::{Drop, DropErrorsIn, DropErrorsOf, DropErrorsWithCause};
pub use raise::{
    Raise, RaiseErrorsIn, RaiseErrorsOf, RaiseErrorsWithCause, RaiseWhen, RaiseWhenErrorsIn,
    RaiseWhenErrorsOf, RaiseWhenErrorsWithCause,
};
pub use replace::{Replace, ReplaceErrorsIn, ReplaceErrorsOf, ReplaceErrorsWithCause};
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
        A::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnErrorOf<A> for O {
    type Output<D>
        = A::Output
    where
        D: Diagnostic,
        A: ErrorPolicyOf<Self, D>;

    fn on_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: ErrorPolicyOf<Self, D>,
    {
        A::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnErrorIn<A> for O {
    type Output<G>
        = A::Output
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<Self, G>;

    fn on_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<Self, G>,
    {
        A::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnErrorWithCause<A> for O {
    type Output<C>
        = A::Output
    where
        C: Error + 'static,
        A: ErrorPolicyWithCause<Self, C>;

    fn on_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: ErrorPolicyWithCause<Self, C>,
    {
        A::build(&policy, self.clone())
    }
}
