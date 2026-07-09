use crate::{
    Bare, Explain, IndexDomain, Indexed, Operand, OrderState, QueryResult, Scalar,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operands::{BareValuesOperand, ValuesOperand},
    operations::{
        Apply, ArgumentSource, ElementKernel, ErrorPolicy, Keyed, Looked, Operation,
        OperationContext, Pipeline, Prepare,
    },
    optimizer::{
        EstimateCost, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, PlanNode, Stats,
    },
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::{fmt, hash::Hasher};

#[derive(Clone, Operation)]
pub struct Replace<A>(pub A);

impl<A: PlanIdentity> PlanIdentity for Replace<A> {
    fn identity_eq(&self, other: &Self) -> bool {
        self.0.identity_eq(&other.0)
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.0.identity_hash(state);
    }
}

impl<A: PlanInputs> PlanInputs for Replace<A> {
    fn inputs(&self) -> Vec<&dyn PlanNode> {
        PlanInputs::inputs(&self.0)
    }
}

impl<A> OptimizerHints for Replace<A> {}

impl<A: PlanIdentity + PlanInputs + 'static> OperationInputs for Replace<A> {
    type Inputs<'a, I: 'a> = (&'a I,);

    fn inputs<'a, I: 'a>(&'a self, primary: &'a I) -> Self::Inputs<'a, I> {
        (primary,)
    }
}

impl<A> Explain for Replace<A> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        fmt::Write::write_str(formatter, "Replace")
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

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        replacement: Self::Prepared<'a>,
    ) -> QueryResult<
        Pipeline<
            'a,
            (I::Index<'a>, QueryResult<GraphRecordValue>),
            (I::Index<'a>, QueryResult<GraphRecordValue>),
        >,
    > {
        Ok(Pipeline::default().map(
            move |(index, result): (I::Index<'a>, QueryResult<GraphRecordValue>)| match result {
                Ok(value) => (index, Ok(value)),
                Err(original) => match R::lookup(&replacement, &index) {
                    Looked::Present(Ok(value)) => (index, Ok(value.clone())),
                    Looked::Present(Err(_)) | Looked::Absent(_) => (index, Err(original)),
                },
            },
        ))
    }
}

impl<I, R, O> EstimateCost<Replace<R>> for ValuesOperand<I, O>
where
    I: IndexDomain,
    O: OrderState,
    for<'a> R: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordValue>,
{
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &Replace<R>,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
    }
}

impl ElementKernel<Bare<Scalar>> for Replace<GraphRecordValue> {
    type OutShape = Bare<Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        replacement: Self::Prepared<'a>,
    ) -> QueryResult<Pipeline<'a, QueryResult<GraphRecordValue>, QueryResult<GraphRecordValue>>>
    {
        Ok(
            Pipeline::default().map(move |result: QueryResult<GraphRecordValue>| {
                result.or_else(|original| replacement.clone().or(Err(original)))
            }),
        )
    }
}

impl<O: OrderState> EstimateCost<Replace<GraphRecordValue>> for BareValuesOperand<O> {
    type OutputCost = <Self as Operand>::Cost;

    fn estimate(
        _operation: &Replace<GraphRecordValue>,
        input_cost: <Self as Operand>::Cost,
        _stats: &Stats,
    ) -> Self::OutputCost {
        input_cost
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
