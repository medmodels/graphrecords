use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Mask, QueryResult,
    capabilities::ValueEquality,
    element::{Pipeline, Preserving},
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare, SetSource},
    optimizer::{
        Estimate, Estimated, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats,
    },
    registry::operation_manifest,
    traits::IsIn,
};
use graphrecords_core::GraphRecord;
use std::hash::Hash;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "IsIn")]
#[plan(optimizer_hints(commutes_with_filter, allows_limit_pushdown, empty = if_all))]
pub struct IsInOperation<A> {
    #[argument]
    argument: A,
}

fn membership_estimate<A: Estimated>(
    operation: &IsInOperation<A>,
    input: Estimate,
    stats: &Stats,
) -> Estimate {
    membership_estimate_from(input, &operation.argument.estimate(stats))
}

fn membership_estimate_from(input: Estimate, set: &Estimate) -> Estimate {
    let selectivity = input
        .distinct
        .zip(set.elements)
        .map(|(distinct, size)| (size as f64 / distinct.max(1) as f64).min(1.0));

    Estimate {
        selectivity,
        ..input.with_unknown_distinct()
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for IsInOperation<A>
where
    I: IndexDomain,
    V: ValueEquality,
    A: SetSource<V>,
    for<'a> V::Value<'a>: Eq + Hash,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        let set = A::set(graphrecord, prepared, Self::LABEL)?;

        Ok(Pipeline::unkeyed(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<V, A> ElementKernel<Bare<V>> for IsInOperation<A>
where
    V: ValueEquality + BareValueDomain,
    A: SetSource<V>,
    for<'a> V::Value<'a>: Eq + Hash,
{
    type Emission = Preserving;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        let set = A::set(graphrecord, prepared, Self::LABEL)?;

        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            outcome.map(|value| set.contains(&value))
        }))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        membership_estimate(self, input, stats)
    }
}

impl<E, A> IsIn<A> for E
where
    IsInOperation<A>: Operation,
    E: Build<IsInOperation<A>>,
{
    type Output = E::Output;

    fn is_in(&self, argument: A) -> Self::Output {
        self.build(IsInOperation { argument })
    }
}

operation_manifest! {
    IsInOperation<A> {
        method: IsIn<A>::is_in;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueEquality>;
            argument: A: SetSource<V>;
            input: Indexed<I, V>;
            output: Indexed<I, Mask>;
            emission: Preserving;
        }

        kernel {
            parameters: <V: ValueEquality + BareValueDomain>;
            argument: A: SetSource<V>;
            input: Bare<V>;
            output: Bare<Mask>;
            emission: Preserving;
        }
    }
}
