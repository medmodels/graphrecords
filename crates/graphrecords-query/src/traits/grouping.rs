use crate::{
    Operand,
    operations::{GroupKey, KeyOperand},
};

pub trait GroupBy<K: GroupKey>: Operand {
    type Output: Operand;

    fn group_by(&self, key: K) -> Self::Output;
}

pub trait Broadcast<K: KeyOperand> {
    type ReturnOperand;

    fn broadcast(&self, key: K) -> Self::ReturnOperand;
}

pub trait Ungroup {
    type ReturnOperand;

    fn ungroup(&self) -> Self::ReturnOperand;
}
