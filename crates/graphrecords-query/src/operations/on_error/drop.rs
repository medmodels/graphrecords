use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, QueryResult, Scalar,
    execution::EvaluationCache,
    operands::{BareValuesOperand, ValuesOperand},
    operations::{
        Apply, BareStream, ErrorPolicy, Kernel, KeyedStream, Operation, OperationContext, Prepare,
    },
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Drop")]
pub struct Drop;

impl Prepare for Drop {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain> Kernel<Indexed<I, Scalar>, Multiple> for Drop {
    type Output = ValuesOperand<I>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Scalar, Multiple>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(values.filter_map(|(index, result)| {
            result.ok().map(|value| (index, Ok(value)))
        })))
    }
}

impl<I: IndexDomain> EstimateCost<Drop> for ValuesOperand<I> {
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &Drop,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl Kernel<Bare<Scalar>, Multiple> for Drop {
    type Output = BareValuesOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Scalar, Multiple>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(values.filter_map(|result| result.ok().map(Ok))))
    }
}

impl EstimateCost<Drop> for BareValuesOperand {
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &Drop,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<I> ErrorPolicy<I> for Drop
where
    I: Apply<Self>,
{
    type Output = <I as Apply<Self>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, Self))
    }
}
