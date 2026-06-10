use crate::{
    EvaluateContext, EvaluateOperand, Explain, Operand, Or, RootOperand,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
    execution::ExecutionContext,
    optimizer::{HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Selectivity, Stats},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use graphrecords_utils::aliases::GrHashMap;
use std::ops::BitOr;

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = BoolMaskOperand<O>)]
#[explain(label = "Or")]
pub struct OrContext<O: RootOperand> {
    #[input]
    left: BoolMaskOperand<O>,
    #[input]
    right: BoolMaskOperand<O>,
}

impl<O: RootOperand> Selectivity for OrContext<O> {
    fn selectivity(&self, stats: &Stats) -> f64 {
        let left = self.left.context().selectivity(stats);
        let right = self.right.context().selectivity(stats);

        left.mul_add(-right, left + right)
    }
}

impl<O: RootOperand> EvaluateContext for OrContext<O> {
    type Operand = BoolMaskOperand<O>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>> {
        let right_values_by_index: GrHashMap<O::Index<'a>, bool> =
            self.right.evaluate(graphrecord, context)?.collect();
        let left_values = self.left.evaluate(graphrecord, context)?;

        Ok(Box::new(left_values.map(move |(index, left_value)| {
            let right_value = right_values_by_index.get(&index).copied().unwrap_or(false);
            (index, left_value || right_value)
        })))
    }
}

impl<O: RootOperand> BoolMaskOperandContext for OrContext<O> {
    type RootOperand = O;
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
