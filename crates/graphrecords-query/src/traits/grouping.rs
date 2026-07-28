use crate::{IndexDomain, Operand};

pub trait GroupBy<K>: Operand {
    type ReturnOperand;

    fn group_by(&self, key: K) -> Self::ReturnOperand;
}

pub trait Having<P> {
    type ReturnOperand;

    fn having(&self, predicate: P) -> Self::ReturnOperand;
}

pub trait Broadcast {
    type ReturnOperand;

    fn broadcast(&self) -> Self::ReturnOperand;
}

pub trait BroadcastVia<I: IndexDomain, A> {
    type ReturnOperand;

    fn broadcast_via(&self, via: A) -> Self::ReturnOperand;
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
