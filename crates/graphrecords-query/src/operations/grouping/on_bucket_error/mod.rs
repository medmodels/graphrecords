pub(super) mod drop;
pub(super) mod raise;

use crate::{
    Diagnostic, ErrorGroup, Expression, Series,
    traits::{OnBucketError, OnBucketErrorIn, OnBucketErrorOf, OnBucketErrorWithCause},
};
pub use drop::{
    DropBucketErrors, DropBucketErrorsIn, DropBucketErrorsOf, DropBucketErrorsWithCause,
};
pub use raise::{
    RaiseBucketErrors, RaiseBucketErrorsIn, RaiseBucketErrorsOf, RaiseBucketErrorsWithCause,
};
use std::error::Error;

pub trait BucketErrorPolicy<E: Expression>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

pub trait BucketErrorPolicyOf<E: Expression, D: Diagnostic>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

pub trait BucketErrorPolicyIn<E: Expression, G: ErrorGroup>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

pub trait BucketErrorPolicyWithCause<E: Expression, C: Error + 'static>: Clone + 'static {
    type Output: Expression;

    fn build(&self, input: E) -> Self::Output;
}

impl<E: Expression> OnBucketError for E {
    type Expression = E;
    type Output<A>
        = A::Output
    where
        A: BucketErrorPolicy<E>;

    fn on_bucket_error<A: BucketErrorPolicy<E>>(&self, policy: A) -> Self::Output<A> {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression> OnBucketError for Series<E> {
    type Expression = E;
    type Output<A>
        = Series<A::Output>
    where
        A: BucketErrorPolicy<E>;

    fn on_bucket_error<A: BucketErrorPolicy<E>>(&self, policy: A) -> Self::Output<A> {
        self.bind(self.expression().on_bucket_error(policy))
    }
}

impl<E: Expression, A> OnBucketErrorOf<A> for E {
    type Expression = E;
    type Output<D>
        = A::Output
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<E, D>;

    fn on_bucket_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<E, D>,
    {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression, A> OnBucketErrorOf<A> for Series<E> {
    type Expression = E;
    type Output<D>
        = Series<A::Output>
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<E, D>;

    fn on_bucket_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<E, D>,
    {
        self.bind(self.expression().on_bucket_error_of(policy))
    }
}

impl<E: Expression, A> OnBucketErrorIn<A> for E {
    type Expression = E;
    type Output<G>
        = A::Output
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<E, G>;

    fn on_bucket_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<E, G>,
    {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression, A> OnBucketErrorIn<A> for Series<E> {
    type Expression = E;
    type Output<G>
        = Series<A::Output>
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<E, G>;

    fn on_bucket_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<E, G>,
    {
        self.bind(self.expression().on_bucket_error_in(policy))
    }
}

impl<E: Expression, A> OnBucketErrorWithCause<A> for E {
    type Expression = E;
    type Output<C>
        = A::Output
    where
        C: Error + 'static,
        A: BucketErrorPolicyWithCause<E, C>;

    fn on_bucket_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: BucketErrorPolicyWithCause<E, C>,
    {
        A::build(&policy, self.clone())
    }
}

impl<E: Expression, A> OnBucketErrorWithCause<A> for Series<E> {
    type Expression = E;
    type Output<C>
        = Series<A::Output>
    where
        C: Error + 'static,
        A: BucketErrorPolicyWithCause<E, C>;

    fn on_bucket_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: BucketErrorPolicyWithCause<E, C>,
    {
        self.bind(self.expression().on_bucket_error_with_cause(policy))
    }
}
