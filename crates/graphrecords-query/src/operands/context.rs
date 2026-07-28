use crate::{
    EvaluateOperand, Explain, Operand, QueryResult,
    execution::EvaluationCache,
    optimizer::{Estimated, OptimizePlan, PlanNode},
};
use graphrecords_core::GraphRecord;

pub trait EvaluateContext {
    type Operand: Operand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<<Self::Operand as EvaluateOperand>::ReturnValue<'a>>;
}

pub trait OperandContext<O: Operand>:
    PlanNode + OptimizePlan<Output = O> + Explain + EvaluateContext<Operand = O> + Estimated
{
}

impl<O, C> OperandContext<O> for C
where
    O: Operand,
    C: PlanNode + OptimizePlan<Output = O> + Explain + EvaluateContext<Operand = O> + Estimated,
{
}
