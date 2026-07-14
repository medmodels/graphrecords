use crate::{
    AttributeName, AttributeSet, Bare, EvaluateOperand, Explain, IndexDomain, IndexValue, Indexed,
    Multiple, Operand, Ordered, QueryResult, Scalar, Single, Unit, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, Kernel, KeyedStream, Operation, OperationContext, Prepare},
    optimizer::{
        Cardinality, EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs,
        Stats, ValueCost,
    },
    traits::Last,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Last")]
pub struct LastOperation;

impl Prepare for LastOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V> Kernel<Indexed<I, V>, Multiple<Ordered>> for LastOperation
where
    I: IndexDomain,
    V: ValueType,
{
    type Output = OperandHandle<Indexed<I, V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.last())
    }
}

impl<I: IndexDomain> EstimateCost<LastOperation>
    for OperandHandle<Indexed<I, Unit>, Multiple<Ordered>>
{
    type OutputCost = <OperandHandle<Indexed<I, Unit>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &LastOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        Cardinality(1)
    }
}

impl<I: IndexDomain> EstimateCost<LastOperation>
    for OperandHandle<Indexed<I, Scalar>, Multiple<Ordered>>
{
    type OutputCost = <OperandHandle<Indexed<I, Scalar>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &LastOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        ValueCost::new(Cardinality(1), Cardinality(1))
    }
}

impl<I: IndexDomain> EstimateCost<LastOperation>
    for OperandHandle<Indexed<I, AttributeName>, Multiple<Ordered>>
{
    type OutputCost = <OperandHandle<Indexed<I, AttributeName>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &LastOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        ValueCost::new(Cardinality(1), Cardinality(1))
    }
}

impl<I: IndexDomain> EstimateCost<LastOperation>
    for OperandHandle<Indexed<I, AttributeSet>, Multiple<Ordered>>
{
    type OutputCost = <OperandHandle<Indexed<I, AttributeSet>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &LastOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        Cardinality(1)
    }
}

impl<I: IndexDomain, E: IndexDomain> EstimateCost<LastOperation>
    for OperandHandle<Indexed<I, IndexValue<E>>, Multiple<Ordered>>
{
    type OutputCost = <OperandHandle<Indexed<I, IndexValue<E>>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &LastOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        ValueCost::new(Cardinality(1), Cardinality(1))
    }
}

impl<V> Kernel<Bare<V>, Multiple<Ordered>> for LastOperation
where
    V: ValueType,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(values.last())
    }
}

impl EstimateCost<LastOperation> for OperandHandle<Bare<Scalar>, Multiple<Ordered>> {
    type OutputCost = <OperandHandle<Bare<Scalar>, Single> as Operand>::Cost;

    fn estimate(
        _operation: &LastOperation,
        _input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        ValueCost::new(Cardinality(1), Cardinality(1))
    }
}

impl<S> Last for S
where
    S: Apply<LastOperation>,
{
    type ReturnOperand = <S as Apply<LastOperation>>::Output;

    fn last(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), LastOperation))
    }
}
