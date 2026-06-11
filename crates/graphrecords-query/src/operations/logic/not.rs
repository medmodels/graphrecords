use crate::{
    EvaluateOperand, Explain, IndexDomain, Indexed, Mask, Multiple, Not, Operand, QueryResult,
    execution::EvaluationCache,
    operands::BoolMaskOperand,
    operations::{Apply, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;
use std::ops::Not as BitNot;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Not")]
pub struct NotOperation;

impl Prepare for NotOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain> Kernel<Indexed<I, Mask>, Multiple> for NotOperation {
    type Output = BoolMaskOperand<I>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Mask, Multiple>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(
            values.map(|(index, value)| (index, value.map(|value| !value))),
        ))
    }
}

impl<I: IndexDomain> EstimateCost<NotOperation> for BoolMaskOperand<I> {
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &NotOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost.negated()
    }
}

impl<O> Not for O
where
    O: Apply<NotOperation>,
{
    type ReturnOperand = <O as Apply<NotOperation>>::Output;

    fn not(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), NotOperation))
    }
}

impl<I: IndexDomain> BitNot for BoolMaskOperand<I> {
    type Output = Self;

    fn not(self) -> Self::Output {
        <Self as Not>::not(&self)
    }
}
