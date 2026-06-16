use crate::{
    Explain, IndexDomain, Indexed, Labeled, Multiple, Operand, QueryResult, Unit,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, ArgumentSource, ElementKernel, Keyed, OnMissing, Operation, OperationContext,
        Pipeline, Prepare,
    },
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Filter,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Filter")]
#[plan(optimizer_hints(commutes_with_filter, distinct, empty = if_any))]
pub struct FilterOperation<M> {
    #[argument]
    mask: M,
}

impl<M: Prepare> Prepare for FilterOperation<M> {
    type Prepared<'a>
        = M::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.mask.prepare(graphrecord, cache)
    }
}

impl<I, M> ElementKernel<Indexed<I, Unit>> for FilterOperation<M>
where
    I: IndexDomain,
    M: ArgumentSource<Keyed<I>, Value = bool>,
{
    type OutShape = Indexed<I, Unit>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<Pipeline<'a, (I::Index<'a>, QueryResult<()>), (I::Index<'a>, QueryResult<()>)>>
    {
        let label = Self::LABEL;

        Ok(Pipeline::default().filter_map(
            move |(index, membership): (I::Index<'a>, QueryResult<()>)| match membership {
                Err(failure) => Some((index, Err(failure))),
                Ok(()) => match M::resolve(&prepared, &index, label, OnMissing::Drop) {
                    Ok(Some(true)) => Some((index, Ok(()))),
                    Ok(Some(false) | None) => None,
                    Err(failure) => Some((index, Err(failure))),
                },
            },
        ))
    }
}

impl<I, M> EstimateCost<FilterOperation<M>> for OperandHandle<Indexed<I, Unit>, Multiple>
where
    I: IndexDomain,
    M: ArgumentSource<Keyed<I>, Value = bool>,
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &FilterOperation<M>,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O, M> Filter<M> for O
where
    O: Apply<FilterOperation<M>>,
    FilterOperation<M>: Operation,
{
    type ReturnOperand = <O as Apply<FilterOperation<M>>>::Output;

    fn filter(&self, mask: M) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            FilterOperation { mask },
        ))
    }
}
