use super::OperandHandle;
use crate::{Bare, BoxedIterator, Definite, IndexDomain, Indexed, Mask, MaskMap, Multiple, Single};

pub type NestedBoolMaskIterator<'a, I, T> = BoxedIterator<'a, (<I as IndexDomain>::Index<'a>, T)>;

pub type NestedBoolMaskOperand<I, T, O> = OperandHandle<Indexed<I, MaskMap<T>>, Multiple<O>>;
pub type NestedBoolOperand<I, T> = OperandHandle<Indexed<I, MaskMap<T>>, Single>;
pub type DefiniteNestedBoolOperand<I, T> = OperandHandle<Indexed<I, MaskMap<T>>, Definite>;
pub type BareNestedBoolMaskOperand<T, O> = OperandHandle<Bare<MaskMap<T>>, Multiple<O>>;
pub type BareNestedBoolOperand<T> = OperandHandle<Bare<MaskMap<T>>, Single>;
pub type DefiniteBareNestedBoolOperand<T> = OperandHandle<Bare<MaskMap<T>>, Definite>;
pub type BoolMaskOperand<I, O> = OperandHandle<Indexed<I, Mask>, Multiple<O>>;
pub type BoolOperand<I> = OperandHandle<Indexed<I, Mask>, Single>;
pub type DefiniteBoolOperand<I> = OperandHandle<Indexed<I, Mask>, Definite>;
pub type BareBoolMaskOperand<O> = OperandHandle<Bare<Mask>, Multiple<O>>;
pub type BareBoolOperand = OperandHandle<Bare<Mask>, Single>;
pub type DefiniteBareBoolOperand = OperandHandle<Bare<Mask>, Definite>;
