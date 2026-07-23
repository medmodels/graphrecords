use crate::{
    Diagnostic, ErrorGroup, Operand,
    operations::{
        Apply, ErrorPolicy, ErrorPolicyIn, ErrorPolicyOf, ErrorPolicyWithCause,
        HasErrorCauseOperation, InErrorGroupOperation, IsErrorKindOperation,
    },
};
use std::error::Error;

pub trait Errors: Operand {
    type ReturnOperand: Operand;

    fn errors(&self) -> Self::ReturnOperand;
}

pub trait ErrorKind: Operand {
    type ReturnOperand: Operand;

    fn kind(&self) -> Self::ReturnOperand;
}

pub trait IsErrorKind: Operand {
    type ReturnOperand<D: Diagnostic>: Operand
    where
        Self: Apply<IsErrorKindOperation<D>>;

    fn is<D: Diagnostic>(&self) -> Self::ReturnOperand<D>
    where
        Self: Apply<IsErrorKindOperation<D>>;
}

pub trait InErrorGroup: Operand {
    type ReturnOperand<G: ErrorGroup>: Operand
    where
        Self: Apply<InErrorGroupOperation<G>>;

    fn in_error_group<G: ErrorGroup>(&self) -> Self::ReturnOperand<G>
    where
        Self: Apply<InErrorGroupOperation<G>>;
}

pub trait HasErrorCause: Operand {
    type ReturnOperand<C: Error + 'static>: Operand
    where
        Self: Apply<HasErrorCauseOperation<C>>;

    fn has_cause<C: Error + 'static>(&self) -> Self::ReturnOperand<C>
    where
        Self: Apply<HasErrorCauseOperation<C>>;
}

pub trait ErrorKindName: Operand {
    type ReturnOperand: Operand;

    fn name(&self) -> Self::ReturnOperand;
}

pub trait OnError: Operand {
    fn on_error<A: ErrorPolicy<Self>>(&self, policy: A) -> A::Output;
}

pub trait OnErrorOf<A>: Operand {
    type Output<D: Diagnostic>: Operand
    where
        A: ErrorPolicyOf<Self, D>;

    fn on_error_of<D: Diagnostic>(&self, policy: A) -> Self::Output<D>
    where
        A: ErrorPolicyOf<Self, D>;
}

pub trait OnErrorIn<A>: Operand {
    type Output<G: ErrorGroup>: Operand
    where
        A: ErrorPolicyIn<Self, G>;

    fn on_error_in<G: ErrorGroup>(&self, policy: A) -> Self::Output<G>
    where
        A: ErrorPolicyIn<Self, G>;
}

pub trait OnErrorWithCause<A>: Operand {
    type Output<C: Error + 'static>: Operand
    where
        A: ErrorPolicyWithCause<Self, C>;

    fn on_error_with_cause<C: Error + 'static>(&self, policy: A) -> Self::Output<C>
    where
        A: ErrorPolicyWithCause<Self, C>;
}
