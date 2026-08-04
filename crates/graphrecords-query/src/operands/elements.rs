use super::OperandHandle;
use crate::{Definite, Indexed, Multiple, Single, Unit};

pub type ElementsOperand<I, O> = OperandHandle<Indexed<I, Unit>, Multiple<O>>;
pub type ElementOperand<I> = OperandHandle<Indexed<I, Unit>, Single>;
pub type DefiniteElementOperand<I> = OperandHandle<Indexed<I, Unit>, Definite>;
