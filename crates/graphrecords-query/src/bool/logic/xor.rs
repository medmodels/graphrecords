use crate::{
    BoxedIterator, Explain, Operand, RootOperand, Xor,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
    execution::ExecutionContext,
    optimizer::{HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Selectivity, Stats},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use graphrecords_utils::aliases::GrHashMap;
use std::ops::BitXor;

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = BoolMaskOperand<O>)]
#[explain(label = "Xor")]
pub struct XorContext<O: RootOperand> {
    #[input]
    left: BoolMaskOperand<O>,
    #[input]
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
