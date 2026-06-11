use crate::{
    EvaluateOperand, Explain, IndexDomain, Indexed, Labeled, Multiple, Operand, QueryResult, Unit,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, ArgumentSource, Kernel, KeyedStream, OnMissing, Operation, OperationContext, Prepare,
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

fn filter_indices<'a, I, M>(
    indices: KeyedStream<'a, I, Unit, Multiple>,
    prepared: M::Prepared<'a>,
    label: &'static str,
) -> KeyedStream<'a, I, Unit, Multiple>
where
    I: IndexDomain,
    M: ArgumentSource<I, Value = bool>,
    M::Prepared<'a>: 'a,
{
    Box::new(
        indices.filter_map(move |(index, membership)| match membership {
            Err(failure) => Some((index, Err(failure))),
            Ok(()) => match M::resolve(&prepared, &index, label, OnMissing::Drop) {
                Ok(Some(true)) => Some((index, Ok(()))),
                Ok(Some(false) | None) => None,
                Err(failure) => Some((index, Err(failure))),
            },
        }),
    )
}

impl<I, M> Kernel<Indexed<I, Unit>, Multiple> for FilterOperation<M>
where
    I: IndexDomain,
    M: ArgumentSource<I, Value = bool>,
{
    type Output = OperandHandle<Indexed<I, Unit>, Multiple>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Unit, Multiple>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(filter_indices::<I, M>(
            values,
            prepared,
            <Self as Labeled>::LABEL,
        ))
    }
}

impl<I, M> EstimateCost<FilterOperation<M>> for OperandHandle<Indexed<I, Unit>, Multiple>
where
    I: IndexDomain,
    M: ArgumentSource<I, Value = bool>,
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
