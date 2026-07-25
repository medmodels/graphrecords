use crate::{
    Bare, Diagnostic, Explain, Failure, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    Scalar,
    execution::EvaluationCache,
    operations::{
        Alignment, Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation,
        OperationContext, Pipeline, Prepare, Retention, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Divide,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    ops::Div,
};

type IndexedDivisionElement<'a, I> = (
    <Keyed<I> as Alignment>::Address<'a>,
    QueryResult<GraphRecordValue>,
);

#[derive(Debug)]
pub struct DivisionByZero {
    pub dividend: GraphRecordValue,
}

impl Display for DivisionByZero {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "cannot divide `{}` by zero", self.dividend)
    }
}

impl Error for DivisionByZero {}

impl Diagnostic for DivisionByZero {
    fn name() -> &'static str {
        "DivisionByZero"
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Divide")]
pub struct DivideOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for DivideOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.argument.prepare(graphrecord, cache)
    }
}

fn is_division_by_zero(dividend: &GraphRecordValue, divisor: &GraphRecordValue) -> bool {
    match (dividend, divisor) {
        (
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) | GraphRecordValue::Duration(_),
            GraphRecordValue::Int(0),
        ) => true,
        (
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_),
            GraphRecordValue::Float(divisor),
        ) => *divisor == 0.0,
        _ => false,
    }
}

fn divide_indexed<'a, I, A>(
    prepared: A::Prepared<'a>,
) -> Pipeline<'a, IndexedDivisionElement<'a, I>, IndexedDivisionElement<'a, I>, A::Retention>
where
    I: IndexDomain,
    A: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordValue>,
    A::Prepared<'a>: 'a,
{
    let label = DivideOperation::<A>::LABEL;

    Pipeline::element_wise(move |(index, item)| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep((index, Err(original)));
            }
        };

        let step = A::resolve(&prepared, &index, label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            let result = resolved.and_then(|argument| {
                if is_division_by_zero(&value, &argument) {
                    return Err(Failure::new_at(
                        label,
                        DivisionByZero { dividend: value },
                        &index,
                    ));
                }

                value
                    .div(argument)
                    .map_err(|error| Failure::new_at(label, error, &index))
            });

            (index, result)
        })
    })
}

fn divide_bare<'a, A>(
    prepared: A::Prepared<'a>,
) -> Pipeline<'a, QueryResult<GraphRecordValue>, QueryResult<GraphRecordValue>, A::Retention>
where
    A: ArgumentSource<Unaligned, Value<'a> = GraphRecordValue>,
    A::Prepared<'a>: 'a,
{
    let label = DivideOperation::<A>::LABEL;

    Pipeline::element_wise(move |item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &(), label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                if is_division_by_zero(&value, &argument) {
                    return Err(Failure::new(label, DivisionByZero { dividend: value }));
                }

                value
                    .div(argument)
                    .map_err(|error| Failure::new(label, error))
            })
        })
    })
}

impl<I, A> ElementKernel<Indexed<I, Scalar>> for DivideOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordValue>,
{
    type OutShape = Indexed<I, Scalar>;
    type Retention = <A as ArgumentSource<Keyed<I>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Scalar>, Self>> {
        Ok(divide_indexed::<I, A>(prepared))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<Scalar>> for DivideOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordValue>,
{
    type OutShape = Bare<Scalar>;
    type Retention = <A as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Scalar>, Self>> {
        Ok(divide_bare::<A>(prepared))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<O, A> Divide<A> for O
where
    DivideOperation<A>: Operation,
    O: Apply<DivideOperation<A>>,
{
    type ReturnOperand = <O as Apply<DivideOperation<A>>>::Output;

    fn divide(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            DivideOperation { argument },
        ))
    }
}
