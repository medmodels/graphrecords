use crate::{
    Diagnostic, ErrorGroup, Expression,
    operations::{
        Apply, BucketErrorPolicy, BucketErrorPolicyIn, BucketErrorPolicyOf,
        BucketErrorPolicyWithCause, ErrorPolicy, ErrorPolicyIn, ErrorPolicyOf,
        ErrorPolicyWithCause, HasErrorCauseOperation, InErrorGroupOperation, IsErrorKindOperation,
        KeyErrorPolicy, KeyErrorPolicyIn, KeyErrorPolicyOf, KeyErrorPolicyWithCause,
    },
};
use std::error::Error;

pub trait Errors {
    type Output;

    fn errors(&self) -> Self::Output;
}

pub trait ErrorKind {
    type Output;

    fn kind(&self) -> Self::Output;
}

pub trait IsErrorKind {
    type Expression: Expression;

    type Output<D>
    where
        D: Diagnostic,
        Self::Expression: Apply<IsErrorKindOperation<D>>;

    fn is<D>(&self) -> Self::Output<D>
    where
        D: Diagnostic,
        Self::Expression: Apply<IsErrorKindOperation<D>>;
}

pub trait InErrorGroup {
    type Expression: Expression;

    type Output<G>
    where
        G: ErrorGroup,
        Self::Expression: Apply<InErrorGroupOperation<G>>;

    fn in_error_group<G>(&self) -> Self::Output<G>
    where
        G: ErrorGroup,
        Self::Expression: Apply<InErrorGroupOperation<G>>;
}

pub trait HasErrorCause {
    type Expression: Expression;

    type Output<E>
    where
        E: Error + 'static,
        Self::Expression: Apply<HasErrorCauseOperation<E>>;

    fn has_cause<E>(&self) -> Self::Output<E>
    where
        E: Error + 'static,
        Self::Expression: Apply<HasErrorCauseOperation<E>>;
}

pub trait ErrorKindName {
    type Output;

    fn name(&self) -> Self::Output;
}

pub trait OnError {
    type Expression: Expression;

    type Output<A>
    where
        A: ErrorPolicy<Self::Expression>;

    fn on_error<A: ErrorPolicy<Self::Expression>>(&self, policy: A) -> Self::Output<A>;
}

pub trait OnErrorOf<A> {
    type Expression: Expression;

    type Output<D>
    where
        D: Diagnostic,
        A: ErrorPolicyOf<Self::Expression, D>;

    fn on_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: ErrorPolicyOf<Self::Expression, D>;
}

pub trait OnErrorIn<A> {
    type Expression: Expression;

    type Output<G>
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<Self::Expression, G>;

    fn on_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<Self::Expression, G>;
}

pub trait OnErrorWithCause<A> {
    type Expression: Expression;

    type Output<E>
    where
        E: Error + 'static,
        A: ErrorPolicyWithCause<Self::Expression, E>;

    fn on_error_with_cause<E>(&self, policy: A) -> Self::Output<E>
    where
        E: Error + 'static,
        A: ErrorPolicyWithCause<Self::Expression, E>;
}

pub trait BucketErrors {
    type Output;

    fn bucket_errors(&self) -> Self::Output;
}

pub trait KeyErrors {
    type Output;

    fn key_errors(&self) -> Self::Output;
}

pub trait OnBucketError {
    type Expression: Expression;

    type Output<A>
    where
        A: BucketErrorPolicy<Self::Expression>;

    fn on_bucket_error<A: BucketErrorPolicy<Self::Expression>>(&self, policy: A)
    -> Self::Output<A>;
}

pub trait OnBucketErrorOf<A> {
    type Expression: Expression;

    type Output<D>
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<Self::Expression, D>;

    fn on_bucket_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<Self::Expression, D>;
}

pub trait OnBucketErrorIn<A> {
    type Expression: Expression;

    type Output<G>
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<Self::Expression, G>;

    fn on_bucket_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<Self::Expression, G>;
}

pub trait OnBucketErrorWithCause<A> {
    type Expression: Expression;

    type Output<E>
    where
        E: Error + 'static,
        A: BucketErrorPolicyWithCause<Self::Expression, E>;

    fn on_bucket_error_with_cause<E>(&self, policy: A) -> Self::Output<E>
    where
        E: Error + 'static,
        A: BucketErrorPolicyWithCause<Self::Expression, E>;
}

pub trait OnKeyError {
    type Expression: Expression;

    type Output<A>
    where
        A: KeyErrorPolicy<Self::Expression>;

    fn on_key_error<A: KeyErrorPolicy<Self::Expression>>(&self, policy: A) -> Self::Output<A>;
}

pub trait OnKeyErrorOf<A> {
    type Expression: Expression;

    type Output<D>
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<Self::Expression, D>;

    fn on_key_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<Self::Expression, D>;
}

pub trait OnKeyErrorIn<A> {
    type Expression: Expression;

    type Output<G>
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<Self::Expression, G>;

    fn on_key_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<Self::Expression, G>;
}

pub trait OnKeyErrorWithCause<A> {
    type Expression: Expression;

    type Output<E>
    where
        E: Error + 'static,
        A: KeyErrorPolicyWithCause<Self::Expression, E>;

    fn on_key_error_with_cause<E>(&self, policy: A) -> Self::Output<E>
    where
        E: Error + 'static,
        A: KeyErrorPolicyWithCause<Self::Expression, E>;
}
