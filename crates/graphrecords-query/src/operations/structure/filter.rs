use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult,
    ValueDomain,
    element::{Dropping, Pipeline, Retention},
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Filter,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Filter")]
#[plan(optimizer_hints(empty = if_all))]
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

impl<I, V, M> ElementKernel<Indexed<I, V>> for FilterOperation<M>
where
    I: IndexDomain,
    V: ValueDomain,
    M: ArgumentSource<Keyed<I>, Mask>,
{
    type Emission = Dropping;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::keyed(move |index, item| {
            let value = match item {
                Ok(value) => value,
                Err(failure) => return Some(Err(failure)),
            };

            let step = M::resolve(&prepared, &index, label);

            match M::Retention::collapse(step) {
                Some(Ok(true)) => Some(Ok(value)),
                Some(Ok(false)) | None => None,
                Some(Err(failure)) => Some(Err(failure)),
            }
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        match self.mask.estimate(stats).selectivity {
            Some(selectivity) => input.scaled(selectivity),
            None => input,
        }
    }
}

impl<V, M> ElementKernel<Bare<V>> for FilterOperation<M>
where
    V: BareValueDomain,
    M: ArgumentSource<Unaligned, Mask>,
{
    type Emission = Dropping;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::new(move |item| {
            let value = match item {
                Ok(value) => value,
                Err(failure) => return Some(Err(failure)),
            };

            let step = M::resolve(&prepared, &(), label);

            match M::Retention::collapse(step) {
                Some(Ok(true)) => Some(Ok(value)),
                Some(Ok(false)) | None => None,
                Some(Err(failure)) => Some(Err(failure)),
            }
        }))
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
    type ReturnOperand = O::Output;

    fn filter(&self, mask: M) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            FilterOperation { mask },
        ))
    }
}

operation_manifest! {
    FilterOperation<M> {
        method: Filter<M>::filter;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain>;
            argument: M: ArgumentSource<Keyed<I>, Mask>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: Dropping;
        }

        kernel {
            parameters: <V: BareValueDomain>;
            argument: M: ArgumentSource<Unaligned, Mask>;
            input: Bare<V>;
            output: Bare<V>;
            emission: Dropping;
        }
    }
}
