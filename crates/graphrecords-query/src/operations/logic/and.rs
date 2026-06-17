use super::combine_masks;
use crate::{
    And, Explain, IndexDomain, Indexed, Labeled, Mask, Operand, QueryResult,
    execution::EvaluationCache,
    operands::BoolMaskOperand,
    operations::{
        Apply, ArgumentSource, ElementKernel, Keyed, Operation, OperationContext, Pipeline, Prepare,
    },
    optimizer::{EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;
use std::ops::BitAnd;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "And")]
pub struct AndOperation<M> {
    #[argument]
    other: M,
}

impl<M: Prepare> Prepare for AndOperation<M> {
    type Prepared<'a> = M::Prepared<'a>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.other.prepare(graphrecord, cache)
    }
}

impl<I, M> ElementKernel<Indexed<I, Mask>> for AndOperation<M>
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
            |left, right| left && right,
        ))
    }
}

impl<I: IndexDomain, M> EstimateCost<AndOperation<M>> for BoolMaskOperand<I>
where
    for<'a> M: ArgumentSource<Keyed<I>, Value<'a> = bool>,
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &AndOperation<M>,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl<O, M> And<M> for O
where
    AndOperation<M>: Operation,
    O: Apply<AndOperation<M>>,
{
    type ReturnOperand = <O as Apply<AndOperation<M>>>::Output;

    fn and(&self, other: M) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), AndOperation { other }))
    }
}

impl<I: IndexDomain, M> BitAnd<M> for BoolMaskOperand<I>
where
    for<'a> M: ArgumentSource<Keyed<I>, Value<'a> = bool>,
{
    type Output = Self;

    fn bitand(self, rhs: M) -> Self::Output {
        self.and(rhs)
    }
}
