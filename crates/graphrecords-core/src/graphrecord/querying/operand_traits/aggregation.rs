use crate::graphrecord::querying::{
    DeepClone,
    group_by::{GroupOperand, GroupedOperand},
    wrapper::Wrapper,
};
use graphrecords_utils::traits::ReadWriteOrPanic;

pub trait Max {
    type ReturnOperand;

    fn max(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Max> Wrapper<O> {
    #[must_use]
    pub fn max(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().max()
    }
}

impl<O: GroupedOperand + Max> Max for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn max(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.max();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}

pub trait Min {
    type ReturnOperand;

    fn min(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Min> Wrapper<O> {
    #[must_use]
    pub fn min(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().min()
    }
}

impl<O: GroupedOperand + Min> Min for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn min(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.min();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}

pub trait Count {
    type ReturnOperand;

    fn count(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Count> Wrapper<O> {
    #[must_use]
    pub fn count(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().count()
    }
}

impl<O: GroupedOperand + Count> Count for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn count(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.count();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}

pub trait Sum {
    type ReturnOperand;

    fn sum(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Sum> Wrapper<O> {
    #[must_use]
    pub fn sum(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().sum()
    }
}

impl<O: GroupedOperand + Sum> Sum for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn sum(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.sum();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}

pub trait Mean {
    type ReturnOperand;

    fn mean(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Mean> Wrapper<O> {
    #[must_use]
    pub fn mean(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().mean()
    }
}

impl<O: GroupedOperand + Mean> Mean for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn mean(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.mean();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}

pub trait Median {
    type ReturnOperand;

    fn median(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Median> Wrapper<O> {
    #[must_use]
    pub fn median(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().median()
    }
}

impl<O: GroupedOperand + Median> Median for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn median(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.median();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}

pub trait Mode {
    type ReturnOperand;

    fn mode(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Mode> Wrapper<O> {
    #[must_use]
    pub fn mode(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().mode()
    }
}

impl<O: GroupedOperand + Mode> Mode for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn mode(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.mode();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}

pub trait Std {
    type ReturnOperand;

    fn std(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Std> Wrapper<O> {
    #[must_use]
    pub fn std(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().std()
    }
}

impl<O: GroupedOperand + Std> Std for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn std(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.std();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}

pub trait Var {
    type ReturnOperand;

    fn var(&mut self) -> Wrapper<Self::ReturnOperand>;
}

impl<O: Var> Wrapper<O> {
    #[must_use]
    pub fn var(&self) -> Wrapper<O::ReturnOperand> {
        self.0.write_or_panic().var()
    }
}

impl<O: GroupedOperand + Var> Var for GroupOperand<O>
where
    Self: DeepClone,
    O::ReturnOperand: GroupedOperand,
    <O::ReturnOperand as GroupedOperand>::Context: From<Self>,
{
    type ReturnOperand = GroupOperand<O::ReturnOperand>;

    fn var(&mut self) -> Wrapper<Self::ReturnOperand> {
        let operand = self.operand.var();

        Wrapper::<GroupOperand<O::ReturnOperand>>::new(self.deep_clone().into(), operand)
    }
}
