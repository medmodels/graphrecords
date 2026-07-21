use super::{combine_bare_masks, combine_masks};
use crate::{
    Arity, Bare, ElementShape, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, Or,
    QueryResult,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;
use std::ops::BitOr;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Or")]
pub struct OrOperation<M> {
    #[argument]
    other: M,
}

impl<M: Prepare> Prepare for OrOperation<M> {
    type Prepared<'a> = M::Prepared<'a>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.other.prepare(graphrecord, cache)
    }
}

impl<I, M> ElementKernel<Indexed<I, Mask>> for OrOperation<M>
where
    I: IndexDomain,
    for<'a> M: ArgumentSource<Keyed<I>, Value<'a> = bool>,
{
    type OutShape = Indexed<I, Mask>;
    type Retention = <M as ArgumentSource<Keyed<I>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Mask>, Self>> {
        Ok(combine_masks::<I, M>(
            prepared,
            Self::LABEL,
            |left, right| left || right,
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

impl<M> ElementKernel<Bare<Mask>> for OrOperation<M>
where
    for<'a> M: ArgumentSource<Unaligned, Value<'a> = bool>,
{
    type OutShape = Bare<Mask>;
    type Retention = <M as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        Ok(combine_bare_masks::<M>(
            prepared,
            Self::LABEL,
            |left, right| left || right,
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

impl<O, M> Or<M> for O
where
    OrOperation<M>: Operation,
    O: Apply<OrOperation<M>>,
{
    type ReturnOperand = <O as Apply<OrOperation<M>>>::Output;

    fn or(&self, other: M) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), OrOperation { other }))
    }
}

impl<S: ElementShape, C: Arity, M> BitOr<M> for OperandHandle<S, C>
where
    Self: Or<M>,
{
    type Output = <Self as Or<M>>::ReturnOperand;

    fn bitor(self, rhs: M) -> Self::Output {
        self.or(rhs)
    }
}
