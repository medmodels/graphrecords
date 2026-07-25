use super::OperandHandle;
use crate::{Bare, Definite, Indexed, Multiple, Scalar, Single};

pub type ValuesOperand<I, O> = OperandHandle<Indexed<I, Scalar>, Multiple<O>>;
pub type BareValuesOperand<O> = OperandHandle<Bare<Scalar>, Multiple<O>>;
pub type ValueOperand<I> = OperandHandle<Indexed<I, Scalar>, Single>;
pub type BareValueOperand = OperandHandle<Bare<Scalar>, Single>;
pub type DefiniteValueOperand<I> = OperandHandle<Indexed<I, Scalar>, Definite>;
pub type DefiniteBareValueOperand = OperandHandle<Bare<Scalar>, Definite>;
