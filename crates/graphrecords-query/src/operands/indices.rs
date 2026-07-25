use super::OperandHandle;
use crate::{Bare, Definite, IndexValue, Indexed, Multiple, Single};

pub type IndicesOperand<I, O> = OperandHandle<Indexed<I, IndexValue<I>>, Multiple<O>>;
pub type IndexOperand<I> = OperandHandle<Indexed<I, IndexValue<I>>, Single>;
pub type DefiniteIndexOperand<I> = OperandHandle<Indexed<I, IndexValue<I>>, Definite>;
pub type BareIndicesOperand<I, O> = OperandHandle<Bare<IndexValue<I>>, Multiple<O>>;
pub type BareIndexOperand<I> = OperandHandle<Bare<IndexValue<I>>, Single>;
pub type DefiniteBareIndexOperand<I> = OperandHandle<Bare<IndexValue<I>>, Definite>;
