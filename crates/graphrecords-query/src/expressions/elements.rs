use super::ExpressionHandle;
use crate::{Definite, Indexed, Multiple, Single, Unit};

pub type ElementsExpression<I, O> = ExpressionHandle<Indexed<I, Unit>, Multiple<O>>;
pub type ElementExpression<I> = ExpressionHandle<Indexed<I, Unit>, Single>;
pub type DefiniteElementExpression<I> = ExpressionHandle<Indexed<I, Unit>, Definite>;
