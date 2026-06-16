use crate::{
    Bare, Explain, IndexDomain, Indexed, Multiple, Operand, Ordered, QueryResult, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, ElementKernel, Operation, OperationContext, Pipeline, Prepare},
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Unordered,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Unordered")]
pub struct UnorderedOperation;

impl Prepare for UnorderedOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, V: ValueType> ElementKernel<Ordered<Indexed<I, V>>> for UnorderedOperation {
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<'a, (I::Index<'a>, QueryResult<V::Value>), (I::Index<'a>, QueryResult<V::Value>)>,
    > {
        Ok(Pipeline::default())
    }
}

impl<I: IndexDomain, V: ValueType> EstimateCost<UnorderedOperation>
    for OperandHandle<Indexed<I, V>, Multiple>
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &UnorderedOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<V: ValueType> ElementKernel<Ordered<Bare<V>>> for UnorderedOperation {
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<Pipeline<'a, QueryResult<V::Value>, QueryResult<V::Value>>> {
        Ok(Pipeline::default())
    }
}

impl<V: ValueType> EstimateCost<UnorderedOperation> for OperandHandle<Bare<V>, Multiple> {
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &UnorderedOperation,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O> Unordered for O
where
    O: Apply<UnorderedOperation>,
{
    type ReturnOperand = <O as Apply<UnorderedOperation>>::Output;

    fn unordered(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UnorderedOperation))
    }
}
