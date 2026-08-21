use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, Failure, IndexDomain, Indexed, Labeled,
    Multiple, OrderState, QueryResult, Single,
    capabilities::ValueOrdering,
    error::comparison::{IncomparableValues, IncomparableValuesAt},
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Maximum,
};
use graphrecords_core::GraphRecord;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
};

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Maximum")]
#[plan(optimizer_hints(empty = if_any))]
pub struct MaximumOperation;

impl<I, V, O> LaneKernel<Indexed<I, V>, Multiple<O>> for MaximumOperation
where
    I: IndexDomain,
    V: ValueOrdering + BareValueDomain,
    O: OrderState,
    V::Owned: Debug + Display + Send + Sync,
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let maximum = values.try_fold(None, |maximum, (address, value)| {
            let value = value?;

            let Some((maximum_address, maximum_value)) = maximum else {
                return Ok(Some((address, value)));
            };

            match V::ordering(&value, &maximum_value) {
                Some(Ordering::Greater) => Ok(Some((address, value))),
                Some(Ordering::Less | Ordering::Equal) => {
                    Ok(Some((maximum_address, maximum_value)))
                }
                None => Err(Failure::new_at_address::<I, _>(
                    IncomparableValuesAt::new(
                        V::into_owned(value),
                        V::into_owned(maximum_value),
                        I::own_index(&I::index(graphrecord, &address)),
                        I::own_index(&I::index(graphrecord, &maximum_address)),
                    ),
                    graphrecord,
                    &address,
                    Self::LABEL,
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
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let maximum = values.try_fold(None, |maximum, value| {
            let value = value?;

            let Some(maximum) = maximum else {
                return Ok(Some(value));
            };

            match V::ordering(&value, &maximum) {
                Some(Ordering::Greater) => Ok(Some(value)),
                Some(Ordering::Less | Ordering::Equal) => Ok(Some(maximum)),
                None => Err(Failure::new(
                    IncomparableValues::new(V::into_owned(value), V::into_owned(maximum)),
                    Self::LABEL,
                )),
            }
        });

        Ok(maximum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<MaximumOperation>> Maximum for E {
    type Output = E::Output;

    fn max(&self) -> Self::Output {
        self.build(MaximumOperation)
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
            output: ExpressionHandle<Bare<V>, Single>;
            where V::Owned: Debug + Display + Send + Sync;
        }

        kernel {
            parameters: <
                V: ValueOrdering + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
            where V::Owned: Debug + Display + Send + Sync;
        }
    }
}
