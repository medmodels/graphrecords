mod drop;
mod raise;

use crate::{
    Diagnostic, ErrorGroup, Operand,
    traits::{OnBucketError, OnBucketErrorIn, OnBucketErrorOf, OnBucketErrorWithCause},
};
pub use drop::{
    DropBucketErrors, DropBucketErrorsIn, DropBucketErrorsOf, DropBucketErrorsWithCause,
};
pub use raise::{
    RaiseBucketErrors, RaiseBucketErrorsIn, RaiseBucketErrorsOf, RaiseBucketErrorsWithCause,
};
use std::error::Error;

pub trait BucketErrorPolicy<I: Operand>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

pub trait BucketErrorPolicyOf<I: Operand, D: Diagnostic>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

pub trait BucketErrorPolicyIn<I: Operand, G: ErrorGroup>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

pub trait BucketErrorPolicyWithCause<I: Operand, C: Error + 'static>: Clone + 'static {
    type Output: Operand;

    fn build(&self, input: I) -> Self::Output;
}

impl<O: Operand> OnBucketError for O {
    fn on_bucket_error<A: BucketErrorPolicy<Self>>(&self, policy: A) -> A::Output {
        A::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnBucketErrorOf<A> for O {
    type Output<D>
        = A::Output
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<Self, D>;

    fn on_bucket_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<Self, D>,
    {
        A::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnBucketErrorIn<A> for O {
    type Output<G>
        = A::Output
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<Self, G>;

    fn on_bucket_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<Self, G>,
    {
        A::build(&policy, self.clone())
    }
}

impl<O: Operand, A> OnBucketErrorWithCause<A> for O {
    type Output<C>
        = A::Output
    where
        C: Error + 'static,
        A: BucketErrorPolicyWithCause<Self, C>;

    fn on_bucket_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: BucketErrorPolicyWithCause<Self, C>,
    {
        A::build(&policy, self.clone())
    }
}
