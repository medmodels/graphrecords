use crate::{
    Diagnostic, ErrorGroup, Operand,
    operations::{
        Apply, BucketErrorPolicy, BucketErrorPolicyIn, BucketErrorPolicyOf,
        BucketErrorPolicyWithCause, ErrorPolicy, ErrorPolicyIn, ErrorPolicyOf,
        ErrorPolicyWithCause, HasErrorCauseOperation, InErrorGroupOperation, IsErrorKindOperation,
        KeyErrorPolicy, KeyErrorPolicyIn, KeyErrorPolicyOf, KeyErrorPolicyWithCause,
    },
};
use std::error::Error;

pub trait Errors: Operand {
    type ReturnOperand;

    fn errors(&self) -> Self::ReturnOperand;
}

pub trait ErrorKind: Operand {
    type ReturnOperand;

    fn kind(&self) -> Self::ReturnOperand;
}

pub trait IsErrorKind: Operand {
    type ReturnOperand<D>
    where
        D: Diagnostic,
        Self: Apply<IsErrorKindOperation<D>>;

    fn is<D>(&self) -> Self::ReturnOperand<D>
    where
        D: Diagnostic,
        Self: Apply<IsErrorKindOperation<D>>;
}

pub trait InErrorGroup: Operand {
    type ReturnOperand<G>
    where
        G: ErrorGroup,
        Self: Apply<InErrorGroupOperation<G>>;

    fn in_error_group<G>(&self) -> Self::ReturnOperand<G>
    where
        G: ErrorGroup,
        Self: Apply<InErrorGroupOperation<G>>;
}

pub trait HasErrorCause: Operand {
    type ReturnOperand<C>
    where
        C: Error + 'static,
        Self: Apply<HasErrorCauseOperation<C>>;

    fn has_cause<C>(&self) -> Self::ReturnOperand<C>
    where
        C: Error + 'static,
        Self: Apply<HasErrorCauseOperation<C>>;
}

pub trait ErrorKindName: Operand {
    type ReturnOperand;

    fn name(&self) -> Self::ReturnOperand;
}

pub trait OnError: Operand {
    fn on_error<A: ErrorPolicy<Self>>(&self, policy: A) -> A::Output;
}

pub trait OnErrorOf<A>: Operand {
    type Output<D>
    where
        D: Diagnostic,
        A: ErrorPolicyOf<Self, D>;

    fn on_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: ErrorPolicyOf<Self, D>;
}

pub trait OnErrorIn<A>: Operand {
    type Output<G>
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<Self, G>;

    fn on_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: ErrorPolicyIn<Self, G>;
}

pub trait OnErrorWithCause<A>: Operand {
    type Output<C>
    where
        C: Error + 'static,
        A: ErrorPolicyWithCause<Self, C>;

    fn on_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: ErrorPolicyWithCause<Self, C>;
}

pub trait BucketErrors: Operand {
    type ReturnOperand;

    fn bucket_errors(&self) -> Self::ReturnOperand;
}

pub trait KeyErrors: Operand {
    type ReturnOperand;

    fn key_errors(&self) -> Self::ReturnOperand;
}

pub trait OnBucketError: Operand {
    fn on_bucket_error<A: BucketErrorPolicy<Self>>(&self, policy: A) -> A::Output;
}

pub trait OnBucketErrorOf<A>: Operand {
    type Output<D>
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<Self, D>;

    fn on_bucket_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: BucketErrorPolicyOf<Self, D>;
}

pub trait OnBucketErrorIn<A>: Operand {
    type Output<G>
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<Self, G>;

    fn on_bucket_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: BucketErrorPolicyIn<Self, G>;
}

pub trait OnBucketErrorWithCause<A>: Operand {
    type Output<C>
    where
        C: Error + 'static,
        A: BucketErrorPolicyWithCause<Self, C>;

    fn on_bucket_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: BucketErrorPolicyWithCause<Self, C>;
}

pub trait OnKeyError: Operand {
    fn on_key_error<A: KeyErrorPolicy<Self>>(&self, policy: A) -> A::Output;
}

pub trait OnKeyErrorOf<A>: Operand {
    type Output<D>
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<Self, D>;

    fn on_key_error_of<D>(&self, policy: A) -> Self::Output<D>
    where
        D: Diagnostic,
        A: KeyErrorPolicyOf<Self, D>;
}

pub trait OnKeyErrorIn<A>: Operand {
    type Output<G>
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<Self, G>;

    fn on_key_error_in<G>(&self, policy: A) -> Self::Output<G>
    where
        G: ErrorGroup,
        A: KeyErrorPolicyIn<Self, G>;
}

pub trait OnKeyErrorWithCause<A>: Operand {
    type Output<C>
    where
        C: Error + 'static,
        A: KeyErrorPolicyWithCause<Self, C>;

    fn on_key_error_with_cause<C>(&self, policy: A) -> Self::Output<C>
    where
        C: Error + 'static,
        A: KeyErrorPolicyWithCause<Self, C>;
}
