use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Mask, QueryResult, ValueDomain,
    element::{Dropping, Pipeline, Retention},
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Filter,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Filter")]
#[plan(optimizer_hints(empty = if_all))]
pub struct FilterOperation<M> {
    #[argument]
    mask: M,
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
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::keyed(move |address, item| {
            let value = match item {
                Ok(value) => value,
                Err(failure) => return Some(Err(failure)),
            };

            let step = M::resolve(graphrecord, &prepared, &address, label);

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
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::new(move |item| {
            let value = match item {
                Ok(value) => value,
                Err(failure) => return Some(Err(failure)),
            };

            let step = M::resolve(graphrecord, &prepared, &(), label);

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

impl<E, M> Filter<M> for E
where
    E: Build<FilterOperation<M>>,
    FilterOperation<M>: Operation,
{
    type Output = E::Output;

    fn filter(&self, mask: M) -> Self::Output {
        self.build(FilterOperation { mask })
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
