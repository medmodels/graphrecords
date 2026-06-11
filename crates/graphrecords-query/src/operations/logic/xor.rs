use super::combine_masks;
use crate::{
    EvaluateOperand, Explain, IndexDomain, Indexed, Labeled, Mask, Multiple, Operand, QueryResult,
    Xor,
    execution::EvaluationCache,
    operands::BoolMaskOperand,
    operations::{
        Apply, ArgumentSource, Kernel, KeyedStream, Operation, OperationContext, Prepare,
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

impl<I, M> Kernel<Indexed<I, Mask>, Multiple> for XorOperation<M>
where
    I: IndexDomain,
    M: ArgumentSource<I, Value = bool>,
{
    type Output = BoolMaskOperand<I>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Mask, Multiple>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(combine_masks::<I, M>(
            values,
            prepared,
            <Self as Labeled>::LABEL,
            |left, right| left ^ right,
        ))
    }
}

impl<I: IndexDomain, M> EstimateCost<XorOperation<M>> for BoolMaskOperand<I>
where
    M: ArgumentSource<I, Value = bool>,
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
    M: ArgumentSource<I, Value = bool>,
{
    type Output = Self;

    fn bitxor(self, rhs: M) -> Self::Output {
        self.xor(rhs)
    }
}
