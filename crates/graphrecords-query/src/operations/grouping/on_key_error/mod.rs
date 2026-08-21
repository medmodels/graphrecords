pub(super) mod drop;
pub(super) mod raise;

use crate::{
    Diagnostic, ErrorGroup, Expression, Series,
    traits::{OnKeyError, OnKeyErrorIn, OnKeyErrorOf, OnKeyErrorWithCause},
};
pub use drop::{DropKeyErrors, DropKeyErrorsIn, DropKeyErrorsOf, DropKeyErrorsWithCause};
pub use raise::{RaiseKeyErrors, RaiseKeyErrorsIn, RaiseKeyErrorsOf, RaiseKeyErrorsWithCause};
use std::error::Error;

pub trait KeyErrorPolicy<E: Expression>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

pub trait KeyErrorPolicyOf<E: Expression, D: Diagnostic>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

pub trait KeyErrorPolicyIn<E: Expression, G: ErrorGroup>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

pub trait KeyErrorPolicyWithCause<E: Expression, C: Error + 'static>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

impl<E: Expression> OnKeyError for E {
    type Expression = E;
    type Output<A>
        = A::Output
    where
        A: KeyErrorPolicy<E>;

    fn on_key_error<A: KeyErrorPolicy<E>>(&self, policy: A) -> Self::Output<A> {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression> OnKeyError for Series<E> {
    type Expression = E;
    type Output<A>
        = Series<A::Output>
    where
        A: KeyErrorPolicy<E>;

    fn on_key_error<A: KeyErrorPolicy<E>>(&self, policy: A) -> Self::Output<A> {
        self.bind(self.expression().on_key_error(policy))
    }
}

impl<E: Expression, A> OnKeyErrorOf<A> for E {
    type Expression = E;
    type Output<D>
        = A::Output
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<E, D>;

    fn on_key_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<E, D>,
    {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression, A> OnKeyErrorOf<A> for Series<E> {
    type Expression = E;
    type Output<D>
        = Series<A::Output>
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<E, D>;

    fn on_key_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<E, D>,
    {
        self.bind(self.expression().on_key_error_of(policy))
    }
}

impl<E: Expression, A> OnKeyErrorIn<A> for E {
    type Expression = E;
    type Output<G>
        = A::Output
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<E, G>;

    fn on_key_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<E, G>,
    {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression, A> OnKeyErrorIn<A> for Series<E> {
    type Expression = E;
    type Output<G>
        = Series<A::Output>
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<E, G>;

    fn on_key_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<E, G>,
    {
        self.bind(self.expression().on_key_error_in(policy))
    }
}

impl<E: Expression, A> OnKeyErrorWithCause<A> for E {
    type Expression = E;
    type Output<C>
        = A::Output
    where
        C: Error + 'static,
        A: KeyErrorPolicyWithCause<E, C>;

    fn on_key_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: KeyErrorPolicyWithCause<E, C>,
    {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression, A> OnKeyErrorWithCause<A> for Series<E> {
    type Expression = E;
    type Output<C>
        = Series<A::Output>
    where
        C: Error + 'static,
        A: KeyErrorPolicyWithCause<E, C>;

    fn on_key_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: KeyErrorPolicyWithCause<E, C>,
    {
        self.bind(self.expression().on_key_error_with_cause(policy))
    }
}
