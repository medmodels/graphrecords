use crate::{
    Bare, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, QueryResult, Scalar,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operands::{BareValuesOperand, ValuesOperand},
    operations::{
        Apply, ArgumentSource, BareStream, ErrorPolicy, Kernel, KeyedStream, Looked, Operation,
        OperationContext, Prepare,
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

impl<I, R> Kernel<Indexed<I, Scalar>, Multiple> for Replace<R>
where
    I: IndexDomain,
    R: ArgumentSource<I, Value = GraphRecordValue>,
{
    type Output = ValuesOperand<I>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Scalar, Multiple>,
        replacement: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(values.map(move |(index, result)| match result {
            Ok(value) => (index, Ok(value)),
            Err(original) => match R::lookup(&replacement, &index) {
                Looked::Present(Ok(value)) => (index, Ok(value.clone())),
                Looked::Present(Err(_)) | Looked::Absent(_) => (index, Err(original)),
            },
        })))
    }
}

impl<I, R> EstimateCost<Replace<R>> for ValuesOperand<I>
where
    I: IndexDomain,
    R: ArgumentSource<I, Value = GraphRecordValue>,
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

impl Kernel<Bare<Scalar>, Multiple> for Replace<GraphRecordValue> {
    type Output = BareValuesOperand;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Scalar, Multiple>,
        replacement: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(Box::new(values.map(move |result| {
            result.or_else(|original| replacement.clone().or(Err(original)))
        })))
    }
}

impl EstimateCost<Replace<GraphRecordValue>> for BareValuesOperand {
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
