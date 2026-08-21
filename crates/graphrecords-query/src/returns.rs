use crate::{
    Arity, EvaluateExpression, Explanation, Expression, IndexDomain, QueryResult, ReturnShape,
    execution::EvaluationCache,
    explain::CompactPlan,
    expressions::{ExpressionHandle, GroupedExpression, Partition, ReturnPartition},
    optimizer::{OptimizationReport, Optimizer, Stats},
};
use graphrecords_core::GraphRecord;
use std::fmt::{self, Display, Formatter};

pub trait ReturnExpression: Clone {
    type ReturnValue<'a>: 'a
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::ReturnValue<'a>>;

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport)
    where
        Self: Sized;

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result;

    fn compact_plan(&self) -> String;
}

pub trait IntoReturn: Expression {
    type Return<'a>: 'a
    where
        Self: 'a;

    fn into_return<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
    ) -> Self::Return<'a>
    where
        Self: 'a;
}

impl<S: ReturnShape, C: Arity> IntoReturn for ExpressionHandle<S, C> {
    type Return<'a>
        = C::Container<'a, S::ReturnElement<'a>>
    where
        Self: 'a;

    fn into_return<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
    ) -> Self::Return<'a>
    where
        Self: 'a,
    {
        C::map_elements(values, |element| {
            S::into_return_element(graphrecord, element)
        })
    }
}

impl<M: IndexDomain, K: IndexDomain, E: IntoReturn> IntoReturn for GroupedExpression<M, K, E> {
    type Return<'a>
        = ReturnPartition<'a, M, K, E::Return<'a>>
    where
        Self: 'a;

    fn into_return<'a>(
        graphrecord: &'a GraphRecord,
        values: Partition<'a, M, K, E>,
    ) -> Self::Return<'a>
    where
        Self: 'a,
    {
        values.into_return_partition(graphrecord, |payload| {
            payload.map(|payload| E::into_return(graphrecord, payload))
        })
    }
}

impl<S: ReturnShape, C: Arity> ReturnExpression for ExpressionHandle<S, C> {
    type ReturnValue<'a>
        = <Self as IntoReturn>::Return<'a>
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        let values = EvaluateExpression::evaluate(self, graphrecord, cache)?;

        Ok(Self::into_return(graphrecord, values))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }

    fn compact_plan(&self) -> String {
        CompactPlan::new(self).to_string()
    }
}

impl<M: IndexDomain, K: IndexDomain, E: IntoReturn> ReturnExpression
    for GroupedExpression<M, K, E>
{
    type ReturnValue<'a>
        = <Self as IntoReturn>::Return<'a>
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        let values = EvaluateExpression::evaluate(self, graphrecord, cache)?;

        Ok(Self::into_return(graphrecord, values))
    }

    fn optimize(self, optimizer: &Optimizer, stats: &Stats) -> (Self, OptimizationReport) {
        optimizer.run_reported(stats, &self)
    }

    fn fmt_plan(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", Explanation::new(self))
    }

    fn compact_plan(&self) -> String {
        CompactPlan::new(self).to_string()
    }
}

pub struct QueryExplanation<'a, R> {
    pub(crate) expression: &'a R,
    pub(crate) report: Option<&'a OptimizationReport>,
}

impl<R: ReturnExpression> Display for QueryExplanation<'_, R> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.expression.fmt_plan(formatter)?;

        if let Some(report) = self.report.filter(|report| !report.phases.is_empty()) {
            write!(formatter, "\n\noptimization:\n{}", report.display())?;
        }

        Ok(())
    }
}
