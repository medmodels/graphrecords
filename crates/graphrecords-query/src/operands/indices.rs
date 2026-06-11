use super::OperandHandle;
use crate::{IndexValue, Indexed, Multiple, Single};

pub type IndicesOperand<I> = OperandHandle<Indexed<I, IndexValue<I>>, Multiple>;
pub type IndexOperand<I> = OperandHandle<Indexed<I, IndexValue<I>>, Single>;
