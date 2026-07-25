use super::OperandHandle;
use crate::{Bare, Definite, FailureKindValue, FailureValue, Indexed, Multiple, Single};

pub type FailuresOperand<I, O> = OperandHandle<Indexed<I, FailureValue>, Multiple<O>>;
pub type FailureKindsOperand<I, O> = OperandHandle<Indexed<I, FailureKindValue>, Multiple<O>>;
pub type BareFailuresOperand<O> = OperandHandle<Bare<FailureValue>, Multiple<O>>;
pub type BareFailureKindsOperand<O> = OperandHandle<Bare<FailureKindValue>, Multiple<O>>;
pub type FailureOperand<I> = OperandHandle<Indexed<I, FailureValue>, Single>;
pub type FailureKindOperand<I> = OperandHandle<Indexed<I, FailureKindValue>, Single>;
pub type BareFailureOperand = OperandHandle<Bare<FailureValue>, Single>;
pub type BareFailureKindOperand = OperandHandle<Bare<FailureKindValue>, Single>;
pub type DefiniteFailureOperand<I> = OperandHandle<Indexed<I, FailureValue>, Definite>;
pub type DefiniteFailureKindOperand<I> = OperandHandle<Indexed<I, FailureKindValue>, Definite>;
pub type DefiniteBareFailureOperand = OperandHandle<Bare<FailureValue>, Definite>;
pub type DefiniteBareFailureKindOperand = OperandHandle<Bare<FailureKindValue>, Definite>;
