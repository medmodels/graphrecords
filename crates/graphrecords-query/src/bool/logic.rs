use crate::{
    And, BoxedIterator, Not, Or, RootOperand, Xor,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use graphrecords_utils::aliases::GrHashMap;
use std::ops::{BitAnd, BitOr, BitXor, Not as BitNot};

pub(super) struct AndContext<O: RootOperand> {
    left: BoolMaskOperand<O>,
    right: BoolMaskOperand<O>,
}

impl<O: RootOperand> BoolMaskOperandContext for AndContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let right_values_by_index: GrHashMap<O::Index<'a>, bool> =
            self.right.evaluate(graphrecord)?.collect();
        let left_values = self.left.evaluate(graphrecord)?;

        Ok(Box::new(left_values.map(move |(index, left_value)| {
            let right_value = right_values_by_index.get(&index).copied().unwrap_or(false);
            (index, left_value && right_value)
        })))
    }
}

impl<O: RootOperand> And for BoolMaskOperand<O> {
    type OtherOperand = Self;
    type ReturnOperand = Self;

    fn and(&self, other: Self::OtherOperand) -> Self::ReturnOperand {
        Self::new(AndContext {
            left: self.clone(),
            right: other,
        })
    }
}

impl<O: RootOperand> BitAnd for BoolMaskOperand<O> {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.and(rhs)
    }
}

pub(super) struct OrContext<O: RootOperand> {
    left: BoolMaskOperand<O>,
    right: BoolMaskOperand<O>,
}

impl<O: RootOperand> BoolMaskOperandContext for OrContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let right_values_by_index: GrHashMap<O::Index<'a>, bool> =
            self.right.evaluate(graphrecord)?.collect();
        let left_values = self.left.evaluate(graphrecord)?;

        Ok(Box::new(left_values.map(move |(index, left_value)| {
            let right_value = right_values_by_index.get(&index).copied().unwrap_or(false);
            (index, left_value || right_value)
        })))
    }
}

impl<O: RootOperand> Or for BoolMaskOperand<O> {
    type OtherOperand = Self;
    type ReturnOperand = Self;

    fn or(&self, other: Self::OtherOperand) -> Self::ReturnOperand {
        Self::new(OrContext {
            left: self.clone(),
            right: other,
        })
    }
}

impl<O: RootOperand> BitOr for BoolMaskOperand<O> {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.or(rhs)
    }
}

pub(super) struct XorContext<O: RootOperand> {
    left: BoolMaskOperand<O>,
    right: BoolMaskOperand<O>,
}

impl<O: RootOperand> BoolMaskOperandContext for XorContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let right_values_by_index: GrHashMap<O::Index<'a>, bool> =
            self.right.evaluate(graphrecord)?.collect();
        let left_values = self.left.evaluate(graphrecord)?;

        Ok(Box::new(left_values.map(move |(index, left_value)| {
            let right_value = right_values_by_index.get(&index).copied().unwrap_or(false);
            (index, left_value ^ right_value)
        })))
    }
}

impl<O: RootOperand> Xor for BoolMaskOperand<O> {
    type OtherOperand = Self;
    type ReturnOperand = Self;

    fn xor(&self, other: Self::OtherOperand) -> Self::ReturnOperand {
        Self::new(XorContext {
            left: self.clone(),
            right: other,
        })
    }
}

impl<O: RootOperand> BitXor for BoolMaskOperand<O> {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        self.xor(rhs)
    }
}

pub(super) struct NotContext<O: RootOperand> {
    parent: BoolMaskOperand<O>,
}

impl<O: RootOperand> BoolMaskOperandContext for NotContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let parent_values = self.parent.evaluate(graphrecord)?;

        Ok(Box::new(
            parent_values.map(|(index, value)| (index, !value)),
        ))
    }
}

impl<O: RootOperand> Not for BoolMaskOperand<O> {
    type ReturnOperand = Self;

    fn not(&self) -> Self::ReturnOperand {
        Self::new(NotContext {
            parent: self.clone(),
        })
    }
}

impl<O: RootOperand> BitNot for BoolMaskOperand<O> {
    type Output = Self;

    fn not(self) -> Self::Output {
        <Self as Not>::not(&self)
    }
}
