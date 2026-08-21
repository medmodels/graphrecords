use crate::{
    Bare, BareValueDomain, Definite, EvaluateExpression, Explain, Failure, IndexDomain, Indexed,
    Labeled, Multiple, QueryResult, Single, Unordered, ValueDomain,
    error::grouping::MissingGroupAggregate,
    expressions::{ExpressionHandle, Partition},
    operations::{Build, GroupKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Broadcast,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Group)]
#[explain(label = "Broadcast")]
#[plan(optimizer_hints(empty = if_any))]
pub struct BroadcastOperation;

impl<M: IndexDomain, K: IndexDomain, I: IndexDomain, V: ValueDomain>
    GroupKernel<M, K, ExpressionHandle<Indexed<I, V>, Single>> for BroadcastOperation
{
    type Output = ExpressionHandle<Indexed<M, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Indexed<I, V>, Single>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        Ok(Box::new(
            buckets
                .into_iter()
                .flat_map(move |(_, members, payload)| {
                    let aggregate = match payload {
                        Ok(Some((_, outcome))) => Some(outcome),
                        Ok(None) => None,
                        Err(failure) => Some(Err(failure)),
                    };

                    members.into_iter().map(move |member| {
                        let outcome = aggregate.clone().unwrap_or_else(|| {
                            Err(Failure::new_at_address::<M, _>(
                                MissingGroupAggregate,
                                graphrecord,
                                &member,
                                Self::LABEL,
                            ))
                        });

                        (member, outcome)
                    })
                })
                .chain(
                    key_failures
                        .into_iter()
                        .map(|(member, failure)| (member, Err(failure))),
                ),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..Estimate::UNKNOWN
        }
    }
}

impl<M: IndexDomain, K: IndexDomain, V: BareValueDomain>
    GroupKernel<M, K, ExpressionHandle<Bare<V>, Single>> for BroadcastOperation
{
    type Output = ExpressionHandle<Indexed<M, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Bare<V>, Single>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        Ok(Box::new(
            buckets
                .into_iter()
                .flat_map(move |(_, members, payload)| {
                    let aggregate = match payload {
                        Ok(Some(outcome)) => Some(outcome),
                        Ok(None) => None,
                        Err(failure) => Some(Err(failure)),
                    };

                    members.into_iter().map(move |member| {
                        let outcome = aggregate.clone().unwrap_or_else(|| {
                            Err(Failure::new_at_address::<M, _>(
                                MissingGroupAggregate,
                                graphrecord,
                                &member,
                                Self::LABEL,
                            ))
                        });

                        (member, outcome)
                    })
                })
                .chain(
                    key_failures
                        .into_iter()
                        .map(|(member, failure)| (member, Err(failure))),
                ),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..Estimate::UNKNOWN
        }
    }
}

impl<M: IndexDomain, K: IndexDomain, I: IndexDomain, V: ValueDomain>
    GroupKernel<M, K, ExpressionHandle<Indexed<I, V>, Definite>> for BroadcastOperation
{
    type Output = ExpressionHandle<Indexed<M, V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Indexed<I, V>, Definite>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        Ok(Box::new(
            buckets
                .into_iter()
                .flat_map(|(_, members, payload)| {
                    let aggregate = match payload {
                        Ok((_, outcome)) => outcome,
                        Err(failure) => Err(failure),
                    };

                    members
                        .into_iter()
                        .map(move |member| (member, aggregate.clone()))
                })
                .chain(
                    key_failures
                        .into_iter()
                        .map(|(member, failure)| (member, Err(failure))),
                ),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..Estimate::UNKNOWN
        }
    }
}

impl<M: IndexDomain, K: IndexDomain, V: BareValueDomain>
    GroupKernel<M, K, ExpressionHandle<Bare<V>, Definite>> for BroadcastOperation
{
    type Output = ExpressionHandle<Indexed<M, V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Bare<V>, Definite>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        Ok(Box::new(
            buckets
                .into_iter()
                .flat_map(|(_, members, payload)| {
                    let aggregate = match payload {
                        Ok(outcome) => outcome,
                        Err(failure) => Err(failure),
                    };

                    members
                        .into_iter()
                        .map(move |member| (member, aggregate.clone()))
                })
                .chain(
                    key_failures
                        .into_iter()
                        .map(|(member, failure)| (member, Err(failure))),
                ),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: input.elements,
            ..Estimate::UNKNOWN
        }
    }
}

impl<E: Build<BroadcastOperation>> Broadcast for E {
    type Output = E::Output;

    fn broadcast(&self) -> Self::Output {
        self.build(BroadcastOperation)
    }
}

operation_manifest! {
    BroadcastOperation {
        method: Broadcast::broadcast;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: ExpressionHandle<Indexed<I, V>, Single>;
            output: ExpressionHandle<Indexed<M, V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain>;
            input: ExpressionHandle<Bare<V>, Single>;
            output: ExpressionHandle<Indexed<M, V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: ExpressionHandle<Indexed<I, V>, Definite>;
            output: ExpressionHandle<Indexed<M, V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain>;
            input: ExpressionHandle<Bare<V>, Definite>;
            output: ExpressionHandle<Indexed<M, V>, Multiple<Unordered>>;
        }
    }
}
