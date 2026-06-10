use crate::{
    BoxedIterator, Explain, Not, Operand, RootOperand,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
    execution::ExecutionContext,
    optimizer::{HasInputs, OptimizeInputs, OptimizerHints, PlanNode, Selectivity, Stats},
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::ops::Not as BitNot;

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = BoolMaskOperand<O>)]
#[explain(label = "Not")]
pub struct NotContext<O: RootOperand> {
    #[input]
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
