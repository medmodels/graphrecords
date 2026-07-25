use super::OperandHandle;
use crate::{
    AttributeName, AttributeSet, Bare, BoxedIterator, Definite, IndexDomain, Indexed, Multiple,
    Single,
};

pub type NestedAttributesIterator<'a, I, T> = BoxedIterator<'a, (<I as IndexDomain>::Index<'a>, T)>;

pub type NestedAttributesOperand<I, O> = OperandHandle<Indexed<I, AttributeSet>, Multiple<O>>;
pub type NestedAttributeOperand<I> = OperandHandle<Indexed<I, AttributeSet>, Single>;
pub type DefiniteNestedAttributeOperand<I> = OperandHandle<Indexed<I, AttributeSet>, Definite>;
pub type BareNestedAttributesOperand<O> = OperandHandle<Bare<AttributeSet>, Multiple<O>>;
pub type BareNestedAttributeOperand = OperandHandle<Bare<AttributeSet>, Single>;
pub type DefiniteBareNestedAttributeOperand = OperandHandle<Bare<AttributeSet>, Definite>;
pub type AttributesOperand<I, O> = OperandHandle<Indexed<I, AttributeName>, Multiple<O>>;
pub type BareAttributesOperand<O> = OperandHandle<Bare<AttributeName>, Multiple<O>>;
pub type AttributeOperand<I> = OperandHandle<Indexed<I, AttributeName>, Single>;
pub type BareAttributeOperand = OperandHandle<Bare<AttributeName>, Single>;
pub type DefiniteAttributeOperand<I> = OperandHandle<Indexed<I, AttributeName>, Definite>;
pub type DefiniteBareAttributeOperand = OperandHandle<Bare<AttributeName>, Definite>;
