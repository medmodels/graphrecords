use crate::{
    AttributeName, Bare, EvaluateOperand, Explain, Failure, IncomparableValues,
    IncomparableValuesAt, IndexDomain, IndexValue, Indexed, Labeled, Multiple, Operand, OrderState,
    QueryResult, Scalar, Single, ValueType,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Max,
    value::IncomparableIndices,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Max")]
pub struct MaxOperation;

impl Prepare for MaxOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

fn maximum_indexed<'a, I, V, O>(
    mut values: KeyedStream<'a, I, V, Multiple<O>>,
) -> <OperandHandle<Indexed<I, V>, Single> as EvaluateOperand>::ReturnValue<'a>
where
    I: IndexDomain,
    V: ValueType,
    O: OrderState,
    for<'b> I::Index<'b>: PartialOrd,
    for<'b> V::Value<'b>: PartialOrd,
    V::Owned: Debug + Display + Send + Sync,
{
    let maximum = values.try_fold(None, |maximum, (index, value)| {
        let value = value.map_err(|failure| (index.clone(), failure))?;

        let Some((maximum_index, maximum_value)) = maximum else {
            return Ok(Some((index, value)));
        };

        match value.partial_cmp(&maximum_value) {
            Some(Ordering::Greater) => return Ok(Some((index, value))),
            Some(Ordering::Less) => return Ok(Some((maximum_index, maximum_value))),
            Some(Ordering::Equal) => {}
            None => {
                let cause = IncomparableValuesAt {
                    first: V::into_owned(value.clone()),
                    second: V::into_owned(maximum_value.clone()),
                    first_element: I::to_owned(&index),
                    second_element: I::to_owned(&maximum_index),
                };
                let failure = Failure::new_at::<I, _>(MaxOperation::LABEL, cause, &index);

                return Err((index, failure));
            }
        }

        match index.partial_cmp(&maximum_index) {
            Some(Ordering::Greater) => Ok(Some((index, value))),
            Some(Ordering::Less | Ordering::Equal) => Ok(Some((maximum_index, maximum_value))),
            None => {
                let cause = IncomparableIndices {
                    value: V::into_owned(value.clone()),
                    first: I::to_owned(&index),
                    second: I::to_owned(&maximum_index),
                };
                let failure = Failure::new_at::<I, _>(MaxOperation::LABEL, cause, &index);

                Err((index, failure))
            }
        }
    });

    match maximum {
        Ok(maximum) => maximum.map(|(index, value)| (index, Ok(value))),
        Err((index, failure)) => Some((index, Err(failure))),
    }
}

fn maximum_bare<'a, V, O>(
    mut values: BareStream<'a, V, Multiple<O>>,
) -> <OperandHandle<Bare<V>, Single> as EvaluateOperand>::ReturnValue<'a>
where
    V: ValueType,
    O: OrderState,
    for<'b> V::Value<'b>: PartialOrd,
    V::Owned: Debug + Display + Send + Sync,
{
    let maximum = values.try_fold(None, |maximum, value| {
        let value = value?;

        let Some(maximum) = maximum else {
            return Ok(Some(value));
        };

        match value.partial_cmp(&maximum) {
            Some(Ordering::Greater) => Ok(Some(value)),
            Some(Ordering::Less | Ordering::Equal) => Ok(Some(maximum)),
            None => Err(Failure::new(
                MaxOperation::LABEL,
                IncomparableValues {
                    first: V::into_owned(value.clone()),
                    second: V::into_owned(maximum.clone()),
                },
            )),
        }
    });

    maximum.transpose()
}

impl<I, O> LaneKernel<Indexed<I, Scalar>, Multiple<O>> for MaxOperation
where
    I: IndexDomain,
    O: OrderState,
    for<'a> I::Index<'a>: PartialOrd,
{
    type Output = OperandHandle<Indexed<I, Scalar>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(maximum_indexed::<I, Scalar, O>(values))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<I, O> LaneKernel<Indexed<I, AttributeName>, Multiple<O>> for MaxOperation
where
    I: IndexDomain,
    O: OrderState,
    for<'a> I::Index<'a>: PartialOrd,
{
    type Output = OperandHandle<Indexed<I, AttributeName>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, AttributeName, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(maximum_indexed::<I, AttributeName, O>(values))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<K, I, O> LaneKernel<Indexed<K, IndexValue<I>>, Multiple<O>> for MaxOperation
where
    K: IndexDomain,
    I: IndexDomain,
    O: OrderState,
    for<'a> K::Index<'a>: PartialOrd,
    I::Owned: PartialOrd,
{
    type Output = OperandHandle<Indexed<K, IndexValue<I>>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, K, IndexValue<I>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(maximum_indexed::<K, IndexValue<I>, O>(values))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: OrderState> LaneKernel<Bare<Scalar>, Multiple<O>> for MaxOperation {
    type Output = OperandHandle<Bare<Scalar>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Scalar, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(maximum_bare::<Scalar, O>(values))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: OrderState> LaneKernel<Bare<AttributeName>, Multiple<O>> for MaxOperation {
    type Output = OperandHandle<Bare<AttributeName>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, AttributeName, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(maximum_bare::<AttributeName, O>(values))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<I, O> LaneKernel<Bare<IndexValue<I>>, Multiple<O>> for MaxOperation
where
    I: IndexDomain,
    O: OrderState,
    I::Owned: PartialOrd,
{
    type Output = OperandHandle<Bare<IndexValue<I>>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, IndexValue<I>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(maximum_bare::<IndexValue<I>, O>(values))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<MaxOperation>> Max for O {
    type ReturnOperand = O::Output;

    fn max(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), MaxOperation))
    }
}
