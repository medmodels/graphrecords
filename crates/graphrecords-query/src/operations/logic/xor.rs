use super::{combine_masks_bare, combine_masks_indexed};
use crate::{
    Arity, Bare, ElementShape, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult,
    Xor,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;
use std::ops::BitXor;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Xor")]
pub struct XorOperation<M> {
    #[argument]
    other: M,
}

impl<M: Prepare> Prepare for XorOperation<M> {
    type Prepared<'a> = M::Prepared<'a>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.other.prepare(graphrecord, cache)
    }
}

impl<I, M> ElementKernel<Indexed<I, Mask>> for XorOperation<M>
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
        Ok(combine_masks_indexed::<I, M>(
            prepared,
            Self::LABEL,
            |left, right| left ^ right,
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

impl<M> ElementKernel<Bare<Mask>> for XorOperation<M>
where
    for<'a> M: ArgumentSource<Unaligned, Value<'a> = bool>,
{
    type OutShape = Bare<Mask>;
    type Retention = <M as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        Ok(combine_masks_bare::<M>(
            prepared,
            Self::LABEL,
            |left, right| left != right,
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

impl<O, M> Xor<M> for O
where
    XorOperation<M>: Operation,
    O: Apply<XorOperation<M>>,
{
    type ReturnOperand = <O as Apply<XorOperation<M>>>::Output;

    fn xor(&self, other: M) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), XorOperation { other }))
    }
}

impl<S: ElementShape, C: Arity, M> BitXor<M> for OperandHandle<S, C>
where
    Self: Xor<M>,
{
    type Output = <Self as Xor<M>>::ReturnOperand;

    fn bitxor(self, rhs: M) -> Self::Output {
        self.xor(rhs)
    }
}
