use crate::{
    And, BoxedIterator, Operand, RootOperand,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
    execution::ExecutionContext,
    optimizer::{Explain, OptimizerHints, PlanNode, Selectivity, Stats},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use graphrecords_utils::aliases::GrHashMap;
use std::ops::BitAnd;

#[derive(PlanNode, OptimizerHints, Explain)]
#[plan_node(operand = BoolMaskOperand<O>)]
#[explain(label = "And")]
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
