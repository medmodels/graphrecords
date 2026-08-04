use super::OperandHandle;
use crate::{Bare, Definite, Indexed, Mask, Multiple, Single};

pub type BoolMaskOperand<I, O> = OperandHandle<Indexed<I, Mask>, Multiple<O>>;
pub type BareBoolMaskOperand<O> = OperandHandle<Bare<Mask>, Multiple<O>>;
pub type BoolOperand<I> = OperandHandle<Indexed<I, Mask>, Single>;
pub type BareBoolOperand = OperandHandle<Bare<Mask>, Single>;
pub type DefiniteBoolOperand<I> = OperandHandle<Indexed<I, Mask>, Definite>;
pub type DefiniteBareBoolOperand = OperandHandle<Bare<Mask>, Definite>;
