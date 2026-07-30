use super::{combine_masks_bare, combine_masks_indexed};
use crate::{
    Arity, Bare, ElementShape, ExclusiveOr, Explain, IndexDomain, Indexed, Labeled, Mask, Operand,
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
use std::ops::BitXor;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Xor")]
#[plan(optimizer_hints(empty = if_all))]
pub struct ExclusiveOrOperation<M> {
    #[argument]
    other: M,
}

impl<M: Prepare> Prepare for ExclusiveOrOperation<M> {
    type Prepared<'a>
        = M::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.other.prepare(graphrecord, cache)
    }
}

impl<I, M> ElementKernel<Indexed<I, Mask>> for ExclusiveOrOperation<M>
where
    I: IndexDomain,
    for<'a> M: ArgumentSource<Keyed<I>, Value<'a> = bool>,
{
    type Emission = M::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Mask>, Self>> {
        Ok(combine_masks_indexed::<_, M>(
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

impl<M> ElementKernel<Bare<Mask>> for ExclusiveOrOperation<M>
where
    for<'a> M: ArgumentSource<Unaligned, Value<'a> = bool>,
{
    type Emission = M::Retention;
    type OutShape = Bare<Mask>;

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

impl<O, M> ExclusiveOr<M> for O
where
    ExclusiveOrOperation<M>: Operation,
    O: Apply<ExclusiveOrOperation<M>>,
{
    type ReturnOperand = O::Output;

    fn xor(&self, other: M) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            ExclusiveOrOperation { other },
        ))
    }
}

impl<S, C, M> BitXor<M> for OperandHandle<S, C>
where
    S: ElementShape,
    C: Arity,
    Self: ExclusiveOr<M>,
{
    type Output = <Self as ExclusiveOr<M>>::ReturnOperand;

    fn bitxor(self, rhs: M) -> Self::Output {
        self.xor(rhs)
    }
}
