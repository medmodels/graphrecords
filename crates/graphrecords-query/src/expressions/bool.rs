use super::ExpressionHandle;
use crate::{Bare, Definite, Indexed, Mask, Multiple, Single};

pub type BoolMaskExpression<I, O> = ExpressionHandle<Indexed<I, Mask>, Multiple<O>>;
pub type BareBoolMaskExpression<O> = ExpressionHandle<Bare<Mask>, Multiple<O>>;
pub type BoolExpression<I> = ExpressionHandle<Indexed<I, Mask>, Single>;
pub type BareBoolExpression = ExpressionHandle<Bare<Mask>, Single>;
pub type DefiniteBoolExpression<I> = ExpressionHandle<Indexed<I, Mask>, Definite>;
pub type DefiniteBareBoolExpression = ExpressionHandle<Bare<Mask>, Definite>;
