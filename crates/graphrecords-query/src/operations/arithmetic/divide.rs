use crate::{
    Bare, Diagnostic, Explain, Failure, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    Scalar,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, BarePipeline, ElementKernel, ElementPipeline, IndexedValuePipeline,
        Keyed, Operation, OperationContext, Pipeline, Prepare, Retention, Unaligned,
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
#[operation(scope = Element)]
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
) -> IndexedValuePipeline<'a, I, Scalar, Scalar, A::Retention>
where
    I: IndexDomain,
    A: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordValue>,
    A::Prepared<'a>: 'a,
{
    let label = DivideOperation::<A>::LABEL;

    Pipeline::keyed(move |index, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &index, label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                if is_division_by_zero(&value, &argument) {
                    return Err(Failure::new_at::<I, _>(
                        label,
                        DivisionByZero { dividend: value },
                        &index,
                    ));
                }

                value
                    .div(argument)
                    .map_err(|error| Failure::new_at::<I, _>(label, error, &index))
            })
        })
    })
}

fn divide_bare<'a, A>(prepared: A::Prepared<'a>) -> BarePipeline<'a, Scalar, Scalar, A::Retention>
where
    A: ArgumentSource<Unaligned, Value<'a> = GraphRecordValue>,
    A::Prepared<'a>: 'a,
{
    let label = DivideOperation::<A>::LABEL;

    Pipeline::new(move |item| {
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
    type Emission = A::Retention;
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Scalar>, Self>> {
        Ok(divide_indexed::<_, A>(prepared))
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
    type Emission = A::Retention;
    type OutShape = Bare<Scalar>;

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
    type ReturnOperand = O::Output;

    fn divide(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            DivideOperation { argument },
        ))
    }
}
