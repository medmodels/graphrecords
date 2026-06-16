use super::combine_masks;
use crate::{
    Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult, Xor,
    execution::EvaluationCache,
    operands::BoolMaskOperand,
    operations::{
        Apply, ArgumentSource, ElementKernel, Keyed, Operation, OperationContext, Pipeline, Prepare,
    },
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
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
    M: ArgumentSource<Keyed<I>, Value = bool>,
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
}

impl<I: IndexDomain, M> EstimateCost<XorOperation<M>> for BoolMaskOperand<I>
where
    M: ArgumentSource<Keyed<I>, Value = bool>,
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &XorOperation<M>,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
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

impl<I: IndexDomain, M> BitXor<M> for BoolMaskOperand<I>
where
    M: ArgumentSource<Keyed<I>, Value = bool>,
{
    type Output = Self;

    fn bitxor(self, rhs: M) -> Self::Output {
        self.xor(rhs)
    }
}
