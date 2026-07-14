use crate::{
    Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult, Unit,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, Keyed, OnMissing, Operation, OperationContext,
        Pipeline, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
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
    for<'a> M: ArgumentSource<Keyed<I>, Value<'a> = bool>,
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

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        match self.mask.estimate(stats).selectivity {
            Some(selectivity) => input.scaled(selectivity),
            None => input,
        }
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
