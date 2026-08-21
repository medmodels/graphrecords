use super::{combine_masks_kleene_bare, combine_masks_kleene_indexed};
use crate::{
    And, Arity, Bare, ElementShape, Explain, IndexDomain, Indexed, Labeled, Mask, QueryResult,
    Series,
    expressions::ExpressionHandle,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
};
use graphrecords_core::GraphRecord;
use std::ops::BitAnd;

const DETERMINING: bool = false;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "And")]
#[plan(optimizer_hints(empty = if_all))]
pub struct AndOperation<M> {
    #[argument]
    other: M,
}

impl<I: IndexDomain, M: ArgumentSource<Keyed<I>, Mask>> ElementKernel<Indexed<I, Mask>>
    for AndOperation<M>
{
    type Emission = M::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Mask>, Self>> {
        Ok(combine_masks_kleene_indexed::<_, M>(
            graphrecord,
            prepared,
            DETERMINING,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.other.estimate(stats).selectivity)
            .map(|(left, right)| left * right);

        Estimate {
            selectivity,
            ..input
        }
    }
}

impl<M: ArgumentSource<Unaligned, Mask>> ElementKernel<Bare<Mask>> for AndOperation<M> {
    type Emission = M::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        Ok(combine_masks_kleene_bare::<M>(
            graphrecord,
            prepared,
            DETERMINING,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.other.estimate(stats).selectivity)
            .map(|(left, right)| left * right);

        Estimate {
            selectivity,
            ..input
        }
    }
}

impl<E, M> And<M> for E
where
    AndOperation<M>: Operation,
    E: Build<AndOperation<M>>,
{
    type Output = E::Output;

    fn and(&self, other: M) -> Self::Output {
        self.build(AndOperation { other })
    }
}

impl<S, C, M> BitAnd<M> for ExpressionHandle<S, C>
where
    S: ElementShape,
    C: Arity,
    Self: And<M>,
{
    type Output = <Self as And<M>>::Output;

    fn bitand(self, rhs: M) -> Self::Output {
        self.and(rhs)
    }
}

impl<E, M> BitAnd<M> for Series<E>
where
    Self: And<M>,
{
    type Output = <Self as And<M>>::Output;

    fn bitand(self, rhs: M) -> Self::Output {
        self.and(rhs)
    }
}

operation_manifest! {
    AndOperation<M> {
        method: And<M>::and;
        scope: element;

        kernel {
            parameters: <I: IndexDomain>;
            argument: M: ArgumentSource<Keyed<I>, Mask>;
            input: Indexed<I, Mask>;
            output: Indexed<I, Mask>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <>;
            argument: M: ArgumentSource<Unaligned, Mask>;
            input: Bare<Mask>;
            output: Bare<Mask>;
            emission: ArgumentRetention;
        }
    }
}
