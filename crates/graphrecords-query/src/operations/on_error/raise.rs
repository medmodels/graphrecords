use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, OrderState,
    QueryResult, Scalar,
    execution::EvaluationCache,
    operands::{BareValuesOperand, ValuesOperand},
    operations::{
        Apply, BareStream, ErrorPolicy, Kernel, KeyedStream, Operation, OperationContext, Prepare,
    },
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Raise")]
pub struct Raise;

impl Prepare for Raise {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain, O: OrderState> Kernel<Indexed<I, Scalar>, Multiple<O>> for Raise {
    type Output = ValuesOperand<I, O>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let raised: Vec<_> = values
            .map(|(index, result)| result.map(|value| (index, value)))
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(
            raised.into_iter().map(|(index, value)| (index, Ok(value))),
        ))
    }
}

impl<I: IndexDomain, O: OrderState> EstimateCost<Raise> for ValuesOperand<I, O> {
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &Raise,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O: OrderState> Kernel<Bare<Scalar>, Multiple<O>> for Raise {
    type Output = BareValuesOperand<O>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let raised: Vec<_> = values.collect::<QueryResult<_>>()?;

        Ok(Box::new(raised.into_iter().map(Ok)))
    }
}

impl<O: OrderState> EstimateCost<Raise> for BareValuesOperand<O> {
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &Raise,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<I> ErrorPolicy<I> for Raise
where
    I: Apply<Self>,
{
    type Output = <I as Apply<Self>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, Self))
    }
}
