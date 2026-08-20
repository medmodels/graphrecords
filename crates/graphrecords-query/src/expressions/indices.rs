use super::ExpressionHandle;
use crate::{Bare, Definite, IndexValue, Indexed, Multiple, Single};

pub type IndicesExpression<I, O> = ExpressionHandle<Indexed<I, IndexValue<I>>, Multiple<O>>;
pub type BareIndicesExpression<I, O> = ExpressionHandle<Bare<IndexValue<I>>, Multiple<O>>;
pub type IndexExpression<I> = ExpressionHandle<Indexed<I, IndexValue<I>>, Single>;
pub type BareIndexExpression<I> = ExpressionHandle<Bare<IndexValue<I>>, Single>;
pub type DefiniteIndexExpression<I> = ExpressionHandle<Indexed<I, IndexValue<I>>, Definite>;
pub type DefiniteBareIndexExpression<I> = ExpressionHandle<Bare<IndexValue<I>>, Definite>;
