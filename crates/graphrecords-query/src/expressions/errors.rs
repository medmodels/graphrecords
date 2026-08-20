use super::ExpressionHandle;
use crate::{Bare, Definite, FailureKindValue, FailureValue, Indexed, Multiple, Single};

pub type FailuresExpression<I, O> = ExpressionHandle<Indexed<I, FailureValue>, Multiple<O>>;
pub type FailureKindsExpression<I, O> = ExpressionHandle<Indexed<I, FailureKindValue>, Multiple<O>>;
pub type BareFailuresExpression<O> = ExpressionHandle<Bare<FailureValue>, Multiple<O>>;
pub type BareFailureKindsExpression<O> = ExpressionHandle<Bare<FailureKindValue>, Multiple<O>>;
pub type FailureExpression<I> = ExpressionHandle<Indexed<I, FailureValue>, Single>;
pub type FailureKindExpression<I> = ExpressionHandle<Indexed<I, FailureKindValue>, Single>;
pub type BareFailureExpression = ExpressionHandle<Bare<FailureValue>, Single>;
pub type BareFailureKindExpression = ExpressionHandle<Bare<FailureKindValue>, Single>;
pub type DefiniteFailureExpression<I> = ExpressionHandle<Indexed<I, FailureValue>, Definite>;
pub type DefiniteFailureKindExpression<I> =
    ExpressionHandle<Indexed<I, FailureKindValue>, Definite>;
pub type DefiniteBareFailureExpression = ExpressionHandle<Bare<FailureValue>, Definite>;
pub type DefiniteBareFailureKindExpression = ExpressionHandle<Bare<FailureKindValue>, Definite>;
