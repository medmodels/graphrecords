use super::{combine_masks_bare, combine_masks_indexed};
use crate::{
    Arity, Bare, ElementShape, ExclusiveOr, Explain, IndexDomain, Indexed, Labeled, Mask,
    QueryResult, Series,
    expressions::ExpressionHandle,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
};
use graphrecords_core::GraphRecord;
use std::ops::BitXor;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "ExclusiveOr")]
#[plan(optimizer_hints(empty = if_all))]
pub struct ExclusiveOrOperation<M> {
    #[argument]
    other: M,
}

impl<I: IndexDomain, M: ArgumentSource<Keyed<I>, Mask>> ElementKernel<Indexed<I, Mask>>
    for ExclusiveOrOperation<M>
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
            |left, right| left ^ right,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.other.estimate(stats).selectivity)
            .map(|(left, right)| (2.0 * left).mul_add(-right, left + right));

        Estimate {
            selectivity,
            ..input
        }
    }
}

impl<M: ArgumentSource<Unaligned, Mask>> ElementKernel<Bare<Mask>> for ExclusiveOrOperation<M> {
    type Emission = M::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        Ok(combine_masks_bare::<M>(
            graphrecord,
            prepared,
            |left, right| left ^ right,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let selectivity = input
            .selectivity
            .zip(self.other.estimate(stats).selectivity)
            .map(|(left, right)| (2.0 * left).mul_add(-right, left + right));

        Estimate {
            selectivity,
            ..input
        }
    }
}

impl<E, M> ExclusiveOr<M> for E
where
    ExclusiveOrOperation<M>: Operation,
    E: Build<ExclusiveOrOperation<M>>,
{
    type Output = E::Output;

    fn xor(&self, other: M) -> Self::Output {
        self.build(ExclusiveOrOperation { other })
    }
}

impl<S, C, M> BitXor<M> for ExpressionHandle<S, C>
where
    S: ElementShape,
    C: Arity,
    Self: ExclusiveOr<M>,
{
    type Output = <Self as ExclusiveOr<M>>::Output;

    fn bitxor(self, rhs: M) -> Self::Output {
        self.xor(rhs)
    }
}

impl<E, M> BitXor<M> for Series<E>
where
    Self: ExclusiveOr<M>,
{
    type Output = <Self as ExclusiveOr<M>>::Output;

    fn bitxor(self, rhs: M) -> Self::Output {
        self.xor(rhs)
    }
}

operation_manifest! {
    ExclusiveOrOperation<M> {
        method: ExclusiveOr<M>::xor;
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
