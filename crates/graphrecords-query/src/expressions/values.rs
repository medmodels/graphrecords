use super::ExpressionHandle;
use crate::{Bare, Definite, Indexed, Multiple, Scalar, Single};

pub type ValuesExpression<I, O> = ExpressionHandle<Indexed<I, Scalar>, Multiple<O>>;
pub type BareValuesExpression<O> = ExpressionHandle<Bare<Scalar>, Multiple<O>>;
pub type ValueExpression<I> = ExpressionHandle<Indexed<I, Scalar>, Single>;
pub type BareValueExpression = ExpressionHandle<Bare<Scalar>, Single>;
pub type DefiniteValueExpression<I> = ExpressionHandle<Indexed<I, Scalar>, Definite>;
pub type DefiniteBareValueExpression = ExpressionHandle<Bare<Scalar>, Definite>;
