use super::{combine_masks_bare, combine_masks_indexed};
use crate::{
    Arity, Bare, ElementShape, Explain, IndexDomain, Indexed, Labeled, Mask, Or, QueryResult,
    Series,
    expressions::ExpressionHandle,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
};
use graphrecords_core::GraphRecord;
use std::ops::BitOr;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Or")]
#[plan(optimizer_hints(empty = if_all))]
pub struct OrOperation<M> {
    #[argument]
    other: M,
}

impl<I: IndexDomain, M: ArgumentSource<Keyed<I>, Mask>> ElementKernel<Indexed<I, Mask>>
    for OrOperation<M>
{
    type Emission = M::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Mask>, Self>> {
        Ok(combine_masks_indexed::<_, M>(
            graphrecord,
            prepared,
            |left, right| left || right,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.other.estimate(stats).selectivity)
            .map(|(left, right)| left.mul_add(-right, left + right));

        Estimate {
            selectivity,
            ..input
        }
    }
}

impl<M: ArgumentSource<Unaligned, Mask>> ElementKernel<Bare<Mask>> for OrOperation<M> {
    type Emission = M::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        Ok(combine_masks_bare::<M>(
            graphrecord,
            prepared,
            |left, right| left || right,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.other.estimate(stats).selectivity)
            .map(|(left, right)| left.mul_add(-right, left + right));

        Estimate {
            selectivity,
            ..input
        }
    }
}

impl<E, M> Or<M> for E
where
    OrOperation<M>: Operation,
    E: Build<OrOperation<M>>,
{
    type Output = E::Output;

    fn or(&self, other: M) -> Self::Output {
        self.build(OrOperation { other })
    }
}

impl<S, C, M> BitOr<M> for ExpressionHandle<S, C>
where
    S: ElementShape,
    C: Arity,
    Self: Or<M>,
{
    type Output = <Self as Or<M>>::Output;

    fn bitor(self, rhs: M) -> Self::Output {
        self.or(rhs)
    }
}

impl<E, M> BitOr<M> for Series<E>
where
    Self: Or<M>,
{
    type Output = <Self as Or<M>>::Output;

    fn bitor(self, rhs: M) -> Self::Output {
        self.or(rhs)
    }
}

operation_manifest! {
    OrOperation<M> {
        method: Or<M>::or;
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
