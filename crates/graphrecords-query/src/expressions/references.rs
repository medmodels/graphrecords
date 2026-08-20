use super::ExpressionHandle;
use crate::{Bare, Definite, EntityReference, IndexValue, Indexed, Multiple, Single};

pub type ReferencesExpression<E, I, O> =
    ExpressionHandle<Indexed<I, EntityReference<E>>, Multiple<O>>;
pub type BareReferencesExpression<E, O> = ExpressionHandle<Bare<EntityReference<E>>, Multiple<O>>;
pub type ReferenceExpression<E, I> = ExpressionHandle<Indexed<I, EntityReference<E>>, Single>;
pub type BareReferenceExpression<E> = ExpressionHandle<Bare<EntityReference<E>>, Single>;
pub type DefiniteReferenceExpression<E, I> =
    ExpressionHandle<Indexed<I, EntityReference<E>>, Definite>;
pub type DefiniteBareReferenceExpression<E> = ExpressionHandle<Bare<EntityReference<E>>, Definite>;
pub type ReferenceIndicesExpression<E, I, O> =
    ExpressionHandle<Indexed<I, IndexValue<E>>, Multiple<O>>;
pub type ReferenceIndexExpression<E, I> = ExpressionHandle<Indexed<I, IndexValue<E>>, Single>;
pub type DefiniteReferenceIndexExpression<E, I> =
    ExpressionHandle<Indexed<I, IndexValue<E>>, Definite>;
