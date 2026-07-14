use crate::{
    Explain, IndexDomain, Indexed, Mask, Not, Operand, OrderState, QueryResult,
    execution::EvaluationCache,
    operands::BoolMaskOperand,
    operations::{Apply, ElementKernel, Operation, OperationContext, Pipeline, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;
use std::ops::Not as BitNot;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Not")]
pub struct NotOperation;

impl Prepare for NotOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I: IndexDomain> ElementKernel<Indexed<I, Mask>> for NotOperation {
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<'a, (I::Index<'a>, QueryResult<bool>), (I::Index<'a>, QueryResult<bool>)>,
    > {
        Ok(
            Pipeline::default().map(|(index, value): (I::Index<'a>, QueryResult<bool>)| {
                (index, value.map(|value| !value))
            }),
        )
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: input.selectivity.map(|selectivity| 1.0 - selectivity),
            ..input
        }
    }
}

impl<O> Not for O
where
    O: Apply<NotOperation>,
{
    type ReturnOperand = <O as Apply<NotOperation>>::Output;

    fn not(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), NotOperation))
    }
}

impl<I: IndexDomain, O: OrderState> BitNot for BoolMaskOperand<I, O> {
    type Output = Self;

    fn not(self) -> Self::Output {
        <Self as Not>::not(&self)
    }
}
