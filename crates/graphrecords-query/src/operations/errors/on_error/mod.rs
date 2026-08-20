pub(super) mod drop;
pub(super) mod raise;
pub(super) mod replace;

use crate::{
    Diagnostic, ErrorGroup, Expression, Series,
    traits::{OnError, OnErrorIn, OnErrorOf, OnErrorWithCause},
};
pub use drop::{Drop, DropErrorsIn, DropErrorsOf, DropErrorsWithCause};
pub use raise::{
    Raise, RaiseErrorsIn, RaiseErrorsOf, RaiseErrorsWithCause, RaiseWhen, RaiseWhenErrorsIn,
    RaiseWhenErrorsOf, RaiseWhenErrorsWithCause,
};
pub use replace::{Replace, ReplaceErrorsIn, ReplaceErrorsOf, ReplaceErrorsWithCause};
use std::error::Error;

pub trait ErrorPolicy<E: Expression>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

pub trait ErrorPolicyOf<E: Expression, D: Diagnostic>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

pub trait ErrorPolicyIn<E: Expression, G: ErrorGroup>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

pub trait ErrorPolicyWithCause<E: Expression, C: Error + 'static>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

impl<E: Expression> OnError for E {
    type Expression = E;
    type Output<A>
        = A::Output
    where
        A: ErrorPolicy<E>;

    fn on_error<A: ErrorPolicy<E>>(&self, policy: A) -> Self::Output<A> {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression> OnError for Series<E> {
    type Expression = E;
    type Output<A>
        = Series<A::Output>
    where
        A: ErrorPolicy<E>;

    fn on_error<A: ErrorPolicy<E>>(&self, policy: A) -> Self::Output<A> {
        self.bind(self.expression().on_error(policy))
    }
}

impl<E: Expression, A> OnErrorOf<A> for E {
    type Expression = E;
    type Output<D>
        = A::Output
    where
        D: Diagnostic,
        A: ErrorPolicyOf<E, D>;

    fn on_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: ErrorPolicyOf<E, D>,
    {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression, A> OnErrorOf<A> for Series<E> {
    type Expression = E;
    type Output<D>
        = Series<A::Output>
    where
        D: Diagnostic,
        A: ErrorPolicyOf<E, D>;

    fn on_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: ErrorPolicyOf<E, D>,
    {
        self.bind(self.expression().on_error_of(policy))
    }
}

impl<E: Expression, A> OnErrorIn<A> for E {
    type Expression = E;
    type Output<G>
        = A::Output
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<E, G>;

    fn on_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<E, G>,
    {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression, A> OnErrorIn<A> for Series<E> {
    type Expression = E;
    type Output<G>
        = Series<A::Output>
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<E, G>;

    fn on_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<E, G>,
    {
        self.bind(self.expression().on_error_in(policy))
    }
}

impl<E: Expression, A> OnErrorWithCause<A> for E {
    type Expression = E;
    type Output<C>
        = A::Output
    where
        C: Error + 'static,
        A: ErrorPolicyWithCause<E, C>;

    fn on_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: ErrorPolicyWithCause<E, C>,
    {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression, A> OnErrorWithCause<A> for Series<E> {
    type Expression = E;
    type Output<C>
        = Series<A::Output>
    where
        C: Error + 'static,
        A: ErrorPolicyWithCause<E, C>;

    fn on_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: ErrorPolicyWithCause<E, C>,
    {
        self.bind(self.expression().on_error_with_cause(policy))
    }
}
