use crate::{
    And, BoxedIterator, Not, Or, RootOperand,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use graphrecords_utils::aliases::GrHashMap;

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

impl<O: RootOperand + 'static> And for BoolMaskOperand<O> {
    type OtherOperand = Self;
    type ReturnOperand = Self;

    fn and(&self, other: Self::OtherOperand) -> Self::ReturnOperand {
        Self::new(AndContext {
            left: self.clone(),
            right: other,
        })
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

impl<O: RootOperand + 'static> Or for BoolMaskOperand<O> {
    type OtherOperand = Self;
    type ReturnOperand = Self;

    fn or(&self, other: Self::OtherOperand) -> Self::ReturnOperand {
        Self::new(OrContext {
            left: self.clone(),
            right: other,
        })
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

impl<O: RootOperand + 'static> Not for BoolMaskOperand<O> {
    type ReturnOperand = Self;

    fn not(&self) -> Self::ReturnOperand {
        Self::new(NotContext {
            parent: self.clone(),
        })
    }
}
