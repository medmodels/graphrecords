use crate::{
    And, BoxedIterator, Not, Operand, Or, RootOperand, Xor,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
    execution::ExecutionContext,
    optimizer::{OptimizerHints, PlanNode, Selectivity, Stats},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use graphrecords_utils::aliases::GrHashMap;
use std::ops::{BitAnd, BitOr, BitXor, Not as BitNot};

#[derive(PlanNode, OptimizerHints)]
#[plan_node(crate = "crate", label = "And", operand = BoolMaskOperand<O>)]
#[optimizer_hints(crate = "crate")]
pub struct AndContext<O: RootOperand> {
    #[plan_node(input)]
    left: BoolMaskOperand<O>,
    #[plan_node(input)]
    right: BoolMaskOperand<O>,
}

impl<O: RootOperand> Selectivity for AndContext<O> {
    fn selectivity(&self, stats: &Stats) -> f64 {
        self.left.context().selectivity(stats) * self.right.context().selectivity(stats)
    }
}

impl<O: RootOperand> BoolMaskOperandContext for AndContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let right_values_by_index: GrHashMap<O::Index<'a>, bool> =
            self.right.evaluate(graphrecord, context)?.collect();
        let left_values = self.left.evaluate(graphrecord, context)?;

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

#[derive(PlanNode, OptimizerHints)]
#[plan_node(crate = "crate", label = "Or", operand = BoolMaskOperand<O>)]
#[optimizer_hints(crate = "crate")]
pub struct OrContext<O: RootOperand> {
    #[plan_node(input)]
    left: BoolMaskOperand<O>,
    #[plan_node(input)]
    right: BoolMaskOperand<O>,
}

impl<O: RootOperand> Selectivity for OrContext<O> {
    fn selectivity(&self, stats: &Stats) -> f64 {
        let left = self.left.context().selectivity(stats);
        let right = self.right.context().selectivity(stats);

        left.mul_add(-right, left + right)
    }
}

impl<O: RootOperand> BoolMaskOperandContext for OrContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let right_values_by_index: GrHashMap<O::Index<'a>, bool> =
            self.right.evaluate(graphrecord, context)?.collect();
        let left_values = self.left.evaluate(graphrecord, context)?;

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

#[derive(PlanNode, OptimizerHints)]
#[plan_node(crate = "crate", label = "Xor", operand = BoolMaskOperand<O>)]
#[optimizer_hints(crate = "crate")]
pub struct XorContext<O: RootOperand> {
    #[plan_node(input)]
    left: BoolMaskOperand<O>,
    #[plan_node(input)]
    right: BoolMaskOperand<O>,
}

impl<O: RootOperand> Selectivity for XorContext<O> {
    fn selectivity(&self, stats: &Stats) -> f64 {
        let left = self.left.context().selectivity(stats);
        let right = self.right.context().selectivity(stats);

        (2.0 * left).mul_add(-right, left + right)
    }
}

impl<O: RootOperand> BoolMaskOperandContext for XorContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let right_values_by_index: GrHashMap<O::Index<'a>, bool> =
            self.right.evaluate(graphrecord, context)?.collect();
        let left_values = self.left.evaluate(graphrecord, context)?;

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

#[derive(PlanNode, OptimizerHints)]
#[plan_node(crate = "crate", label = "Not", operand = BoolMaskOperand<O>)]
#[optimizer_hints(crate = "crate")]
pub struct NotContext<O: RootOperand> {
    #[plan_node(input)]
    input: BoolMaskOperand<O>,
}

impl<O: RootOperand> Selectivity for NotContext<O> {
    fn selectivity(&self, stats: &Stats) -> f64 {
        1.0 - self.input.context().selectivity(stats)
    }
}

impl<O: RootOperand> BoolMaskOperandContext for NotContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let input_values = self.input.evaluate(graphrecord, context)?;

        Ok(Box::new(input_values.map(|(index, value)| (index, !value))))
    }
}

impl<O: RootOperand> Not for BoolMaskOperand<O> {
    type ReturnOperand = Self;

    fn not(&self) -> Self::ReturnOperand {
        Self::new(NotContext {
            input: self.clone(),
        })
    }
}

impl<O: RootOperand> BitNot for BoolMaskOperand<O> {
    type Output = Self;

    fn not(self) -> Self::Output {
        <Self as Not>::not(&self)
    }
}
