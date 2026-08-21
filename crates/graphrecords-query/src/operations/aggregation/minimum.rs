use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, Failure, IndexDomain, Indexed, Labeled,
    Multiple, OrderState, QueryResult, Single,
    capabilities::ValueOrdering,
    error::comparison::{IncomparableValues, IncomparableValuesAt},
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Minimum,
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
#[explain(label = "Minimum")]
#[plan(optimizer_hints(empty = if_any))]
pub struct MinimumOperation;

impl<I, V, O> LaneKernel<Indexed<I, V>, Multiple<O>> for MinimumOperation
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
        let minimum = values.try_fold(None, |minimum, (address, value)| {
            let value = value?;

            let Some((minimum_address, minimum_value)) = minimum else {
                return Ok(Some((address, value)));
            };

            match V::ordering(&value, &minimum_value) {
                Some(Ordering::Less) => Ok(Some((address, value))),
                Some(Ordering::Equal | Ordering::Greater) => {
                    Ok(Some((minimum_address, minimum_value)))
                }
                None => Err(Failure::new_at_address::<I, _>(
                    IncomparableValuesAt::new(
                        V::into_owned(value),
                        V::into_owned(minimum_value),
                        I::own_index(&I::index(graphrecord, &address)),
                        I::own_index(&I::index(graphrecord, &minimum_address)),
                    ),
                    graphrecord,
                    &address,
                    Self::LABEL,
                )),
            }
        });

        Ok(match minimum {
            Ok(minimum) => minimum.map(|(_, value)| Ok(value)),
            Err(failure) => Some(Err(failure)),
        })
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for MinimumOperation
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
        let minimum = values.try_fold(None, |minimum, value| {
            let value = value?;

            let Some(minimum) = minimum else {
                return Ok(Some(value));
            };

            match V::ordering(&value, &minimum) {
                Some(Ordering::Less) => Ok(Some(value)),
                Some(Ordering::Equal | Ordering::Greater) => Ok(Some(minimum)),
                None => Err(Failure::new(
                    IncomparableValues::new(V::into_owned(value), V::into_owned(minimum)),
                    Self::LABEL,
                )),
            }
        });

        Ok(minimum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<MinimumOperation>> Minimum for E {
    type Output = E::Output;

    fn min(&self) -> Self::Output {
        self.build(MinimumOperation)
    }
}

operation_manifest! {
    MinimumOperation {
        method: Minimum::min;
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
