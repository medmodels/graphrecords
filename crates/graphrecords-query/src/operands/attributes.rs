use super::OperandHandle;
use crate::{Bare, Definite, Indexed, Multiple, Single};
use graphrecords_core::graphrecord::AttributeName;

pub type AttributesOperand<I, O> = OperandHandle<Indexed<I, AttributeName>, Multiple<O>>;
pub type BareAttributesOperand<O> = OperandHandle<Bare<AttributeName>, Multiple<O>>;
pub type AttributeOperand<I> = OperandHandle<Indexed<I, AttributeName>, Single>;
pub type BareAttributeOperand = OperandHandle<Bare<AttributeName>, Single>;
pub type DefiniteAttributeOperand<I> = OperandHandle<Indexed<I, AttributeName>, Definite>;
pub type DefiniteBareAttributeOperand = OperandHandle<Bare<AttributeName>, Definite>;
