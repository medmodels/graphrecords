use super::OperandHandle;
use crate::{Bare, Definite, EntityReference, IndexValue, Indexed, Multiple, Single};

pub type ReferencesOperand<E, I, O> = OperandHandle<Indexed<I, EntityReference<E>>, Multiple<O>>;
pub type BareReferencesOperand<E, O> = OperandHandle<Bare<EntityReference<E>>, Multiple<O>>;
pub type ReferenceOperand<E, I> = OperandHandle<Indexed<I, EntityReference<E>>, Single>;
pub type BareReferenceOperand<E> = OperandHandle<Bare<EntityReference<E>>, Single>;
pub type DefiniteReferenceOperand<E, I> = OperandHandle<Indexed<I, EntityReference<E>>, Definite>;
pub type DefiniteBareReferenceOperand<E> = OperandHandle<Bare<EntityReference<E>>, Definite>;
pub type ReferenceIndicesOperand<E, I, O> = OperandHandle<Indexed<I, IndexValue<E>>, Multiple<O>>;
pub type ReferenceIndexOperand<E, I> = OperandHandle<Indexed<I, IndexValue<E>>, Single>;
pub type DefiniteReferenceIndexOperand<E, I> = OperandHandle<Indexed<I, IndexValue<E>>, Definite>;
