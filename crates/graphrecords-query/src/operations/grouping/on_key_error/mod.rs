mod drop;
mod raise;

pub use drop::{DropKeyErrors, DropKeyErrorsIn, DropKeyErrorsOf, DropKeyErrorsWithCause};
pub use raise::{RaiseKeyErrors, RaiseKeyErrorsIn, RaiseKeyErrorsOf, RaiseKeyErrorsWithCause};

use crate::{
    Diagnostic, ErrorGroup, Operand,
    traits::{OnKeyError, OnKeyErrorIn, OnKeyErrorOf, OnKeyErrorWithCause},
};
use std::error::Error;

pub trait KeyErrorPolicy<I: Operand>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

pub trait KeyErrorPolicyOf<I: Operand, D: Diagnostic>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

pub trait KeyErrorPolicyIn<I: Operand, G: ErrorGroup>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

pub trait KeyErrorPolicyWithCause<I: Operand, C: Error + 'static>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

impl<O: Operand> OnKeyError for O {
    fn on_key_error<A: KeyErrorPolicy<Self>>(&self, policy: A) -> A::Output {
        A::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnKeyErrorOf<A> for O {
    type Output<D>
        = A::Output
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<Self, D>;

    fn on_key_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<Self, D>,
    {
        A::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnKeyErrorIn<A> for O {
    type Output<G>
        = A::Output
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<Self, G>;

    fn on_key_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<Self, G>,
    {
        A::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnKeyErrorWithCause<A> for O {
    type Output<C>
        = A::Output
    where
        C: Error + 'static,
        A: KeyErrorPolicyWithCause<Self, C>;

    fn on_key_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: KeyErrorPolicyWithCause<Self, C>,
    {
        A::build(&policy, self.clone())
    }
}
