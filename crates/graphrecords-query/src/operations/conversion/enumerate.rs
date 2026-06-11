use crate::{
    Bare, EvaluateOperand, Explain, Indexed, Multiple, Operand, Positional, QueryResult, ValueType,
    execution::{Cacheable, EvaluationCache},
    operands::OperandHandle,
    operations::{Apply, BareStream, CacheContext, Kernel, Operation, OperationContext, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Enumerate,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Enumerate")]
#[plan(optimizer_hints(distinct))]
pub struct EnumerateOperation;

impl Prepare for EnumerateOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<V: ValueType> Kernel<Bare<V>, Multiple> for EnumerateOperation {
    type Output = OperandHandle<Indexed<Positional, V>, Multiple>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(values.enumerate()))
    }
}

impl<V: ValueType> EstimateCost<EnumerateOperation> for OperandHandle<Bare<V>, Multiple> {
    type OutputCost = <OperandHandle<Indexed<Positional, V>, Multiple> as Operand>::Cost;

    fn estimate(
        _operation: &EnumerateOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O> Enumerate for O
where
    O: Apply<EnumerateOperation>,
    for<'a> <<O as Apply<EnumerateOperation>>::Output as EvaluateOperand>::ReturnValue<'a>:
        Cacheable<'a>,
{
    type ReturnOperand = <O as Apply<EnumerateOperation>>::Output;

    fn enumerate(&self) -> Self::ReturnOperand {
        let enumerated =
            Self::ReturnOperand::new(OperationContext::new(self.clone(), EnumerateOperation));

        Self::ReturnOperand::new(CacheContext::new(enumerated))
    }
}
