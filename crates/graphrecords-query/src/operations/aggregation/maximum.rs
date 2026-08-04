use crate::{
    Bare, BareValueDomain, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled,
    Multiple, Operand, OrderState, QueryResult, Single,
    capabilities::ValueOrdering,
    error::comparison::{IncomparableValues, IncomparableValuesAt},
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Maximum,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Max")]
#[plan(optimizer_hints(empty = if_any))]
pub struct MaximumOperation;

impl Prepare for MaximumOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V, O> LaneKernel<Indexed<I, V>, Multiple<O>> for MaximumOperation
where
    I: IndexDomain,
    V: ValueOrdering + BareValueDomain,
    O: OrderState,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let maximum = values.try_fold(None, |maximum, (index, value)| {
            let value = value?;

            let Some((maximum_index, maximum_value)) = maximum else {
                return Ok(Some((index, value)));
            };

            match V::ordering(&value, &maximum_value) {
                Some(Ordering::Greater) => Ok(Some((index, value))),
                Some(Ordering::Less | Ordering::Equal) => Ok(Some((maximum_index, maximum_value))),
                None => Err(Failure::new_at::<I, _>(
                    Self::LABEL,
                    IncomparableValuesAt::new(
                        V::into_owned(value),
                        V::into_owned(maximum_value),
                        I::to_owned(&index),
                        I::to_owned(&maximum_index),
                    ),
                    &index,
                )),
            }
        });

        Ok(match maximum {
            Ok(maximum) => maximum.map(|(_, value)| Ok(value)),
            Err(failure) => Some(Err(failure)),
        })
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for MaximumOperation
where
    V: ValueOrdering + BareValueDomain,
    O: OrderState,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let maximum = values.try_fold(None, |maximum, value| {
            let value = value?;

            let Some(maximum) = maximum else {
                return Ok(Some(value));
            };

            match V::ordering(&value, &maximum) {
                Some(Ordering::Greater) => Ok(Some(value)),
                Some(Ordering::Less | Ordering::Equal) => Ok(Some(maximum)),
                None => Err(Failure::new(
                    Self::LABEL,
                    IncomparableValues::new(V::into_owned(value), V::into_owned(maximum)),
                )),
            }
        });

        Ok(maximum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<MaximumOperation>> Maximum for O {
    type ReturnOperand = O::Output;

    fn max(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), MaximumOperation))
    }
}

operation_manifest! {
    MaximumOperation {
        method: Maximum::max;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueOrdering + BareValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: OperandHandle<Bare<V>, Single>;
            where V::Owned: Debug + Display + Send + Sync;
        }

        kernel {
            parameters: <
                V: ValueOrdering + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: OperandHandle<Bare<V>, Single>;
            where V::Owned: Debug + Display + Send + Sync;
        }
    }
}
