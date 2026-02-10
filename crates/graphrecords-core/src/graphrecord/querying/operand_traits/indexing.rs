use crate::graphrecord::querying::{
    DeepClone,
    group_by::{GroupOperand, GroupedOperand},
    wrapper::Wrapper,
};
use graphrecords_utils::traits::ReadWriteOrPanic;

pub trait Index {
    type ReturnOperand;

    fn index(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Index> Wrapper<O> {
    #[must_use]
    pub fn index(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().index()
    }
}

impl<O: GroupedOperand + Index> Index for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn index(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.index();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}
