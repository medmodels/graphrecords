use crate::{Operand, operations::KeyOperand};

pub trait GroupBy<K: KeyOperand>: Operand {
    type ReturnOperand: Operand;

    fn group_by(&self, key: K) -> Self::ReturnOperand;
}

pub trait Having<P> {
    type ReturnOperand;

    fn having(&self, predicate: P) -> Self::ReturnOperand;
}

pub trait Broadcast<K: KeyOperand> {
    type ReturnOperand;

    fn broadcast(&self, key: K) -> Self::ReturnOperand;
}

pub trait Keys {
    type ReturnOperand;

    fn keys(&self) -> Self::ReturnOperand;
}

pub trait Ungroup {
    type ReturnOperand;

    fn ungroup(&self) -> Self::ReturnOperand;
}

pub trait UngroupKeyed {
    type ReturnOperand;

    fn ungroup_keyed(&self) -> Self::ReturnOperand;
}
