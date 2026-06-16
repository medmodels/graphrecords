use crate::{
    Bare, Explain, IndexDomain, Indexed, Operand, QueryResult, Scalar,
    execution::EvaluationCache,
    operands::{BareValuesOperand, ValuesOperand},
    operations::{
        Apply, ElementKernel, ErrorPolicy, Operation, OperationContext, Pipeline, Prepare,
    },
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};

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

impl<I: IndexDomain> ElementKernel<Indexed<I, Scalar>> for Drop {
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<
            'a,
            (I::Index<'a>, QueryResult<GraphRecordValue>),
            (I::Index<'a>, QueryResult<GraphRecordValue>),
        >,
    > {
        Ok(Pipeline::default().filter_map(
            |(index, result): (I::Index<'a>, QueryResult<GraphRecordValue>)| {
                result.ok().map(|value| (index, Ok(value)))
            },
        ))
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

impl ElementKernel<Bare<Scalar>> for Drop {
    type OutShape = Bare<Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<Pipeline<'a, QueryResult<GraphRecordValue>, QueryResult<GraphRecordValue>>>
    {
        Ok(Pipeline::default()
            .filter_map(|result: QueryResult<GraphRecordValue>| result.ok().map(Ok)))
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
