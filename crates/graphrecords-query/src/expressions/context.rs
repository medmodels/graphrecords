use crate::{
    EvaluateExpression, Explain, Expression, QueryResult,
    execution::EvaluationCache,
    optimizer::{Estimated, OptimizePlan, PlanNode},
};
use graphrecords_core::GraphRecord;

pub trait EvaluateContext {
    type Expression: Expression;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<<Self::Expression as EvaluateExpression>::ReturnValue<'a>>;
}

pub trait ExpressionContext<E: Expression>:
    PlanNode
    + OptimizePlan<Output = E>
    + Explain
    + EvaluateContext<Expression = E>
    + Estimated
    + Send
    + Sync
{
}

impl<E, C> ExpressionContext<E> for C
where
    E: Expression,
    C: PlanNode
        + OptimizePlan<Output = E>
        + Explain
        + EvaluateContext<Expression = E>
        + Estimated
        + Send
        + Sync,
{
}
