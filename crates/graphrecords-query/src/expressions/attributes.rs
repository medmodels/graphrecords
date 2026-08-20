use super::ExpressionHandle;
use crate::{Bare, Definite, Indexed, Multiple, Single};
use graphrecords_core::graphrecord::AttributeName;

pub type AttributesExpression<I, O> = ExpressionHandle<Indexed<I, AttributeName>, Multiple<O>>;
pub type BareAttributesExpression<O> = ExpressionHandle<Bare<AttributeName>, Multiple<O>>;
pub type AttributeExpression<I> = ExpressionHandle<Indexed<I, AttributeName>, Single>;
pub type BareAttributeExpression = ExpressionHandle<Bare<AttributeName>, Single>;
pub type DefiniteAttributeExpression<I> = ExpressionHandle<Indexed<I, AttributeName>, Definite>;
pub type DefiniteBareAttributeExpression = ExpressionHandle<Bare<AttributeName>, Definite>;
