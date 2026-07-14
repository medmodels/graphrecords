use super::combine_masks;
use crate::{
    Explain, IndexDomain, Indexed, Labeled, Mask, Operand, OrderState, QueryResult, Xor,
    execution::EvaluationCache,
    operands::BoolMaskOperand,
    operations::{
        Apply, ArgumentSource, ElementKernel, Keyed, Operation, OperationContext, Pipeline, Prepare,
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

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<'a, (I::Index<'a>, QueryResult<bool>), (I::Index<'a>, QueryResult<bool>)>,
    > {
        Ok(combine_masks::<I, M>(
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

impl<I: IndexDomain, M, O: OrderState> BitXor<M> for BoolMaskOperand<I, O>
where
    for<'a> M: ArgumentSource<Keyed<I>, Value<'a> = bool>,
{
    type Output = Self;

    fn bitxor(self, rhs: M) -> Self::Output {
        self.xor(rhs)
    }
}
