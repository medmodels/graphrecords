use crate::{
    Bare, Explain, IndexDomain, Indexed, Operand, QueryResult, Scalar,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, ErrorPolicy, Keyed, Lookup,
        Operation, OperationContext, Pipeline, Prepare, Preserving,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::fmt;

#[derive(Clone, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
pub struct Replace<A>(#[argument] pub A);

impl<A: Explain> Explain for Replace<A> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        fmt::Write::write_str(formatter, "Replace")?;
        formatter.labeled_child("replacement", &self.0);

        Ok(())
    }
}

impl<R: Prepare> Prepare for Replace<R> {
    type Prepared<'a>
        = R::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.0.prepare(graphrecord, cache)
    }
}

impl<I, R> ElementKernel<Indexed<I, Scalar>> for Replace<R>
where
    I: IndexDomain,
    for<'a> R: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordValue>,
{
    type OutShape = Indexed<I, Scalar>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        replacement: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Scalar>, Self>> {
        Ok(Pipeline::default().map(
            move |(index, result): (I::Index<'a>, QueryResult<GraphRecordValue>)| match result {
                Ok(value) => (index, Ok(value)),
                Err(original) => match R::lookup(&replacement, &index) {
                    Lookup::Present(Ok(value)) => (index, Ok(value.clone())),
                    Lookup::Present(Err(_)) | Lookup::Absent(_) => (index, Err(original)),
                },
            },
        ))
    }
}

impl ElementKernel<Bare<Scalar>> for Replace<GraphRecordValue> {
    type OutShape = Bare<Scalar>;
    type Retention = Preserving;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        replacement: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Scalar>, Self>> {
        Ok(
            Pipeline::default().map(move |result: QueryResult<GraphRecordValue>| {
                result.or_else(|original| replacement.clone().or(Err(original)))
            }),
        )
    }
}

impl<I, A> ErrorPolicy<I> for Replace<A>
where
    A: Clone + 'static,
    Self: Operation,
    I: Apply<Self>,
{
    type Output = <I as Apply<Self>>::Output;

    fn build(&self, input: I) -> Self::Output {
        Self::Output::new(OperationContext::new(input, self.clone()))
    }
}
