use crate::{
    Bare, Definite, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple,
    Operand, QueryResult, Single, Unordered, ValueType,
    error::grouping::MissingGroupAggregate,
    execution::EvaluationCache,
    index::GroupKey,
    operands::{OperandHandle, Partition},
    operations::{Apply, GroupKernel, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Broadcast,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "Broadcast")]
#[plan(optimizer_hints(empty = if_all))]
pub struct BroadcastOperation;

impl Prepare for BroadcastOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<M: IndexDomain, K: GroupKey, I: IndexDomain, V: ValueType>
    GroupKernel<M, K, OperandHandle<Indexed<I, V>, Single>> for BroadcastOperation
{
    type Output = OperandHandle<Indexed<M, V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Indexed<I, V>, Single>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        Ok(Box::new(
            buckets
                .into_iter()
                .flat_map(|(_, members, payload)| {
                    let aggregate = match payload {
                        Ok(Some((_, outcome))) => Some(outcome),
                        Ok(None) => None,
                        Err(failure) => Some(Err(failure)),
                    };

                    members.into_iter().map(move |member| {
                        let outcome = aggregate.clone().unwrap_or_else(|| {
                            Err(Failure::new_at::<M, _>(
                                Self::LABEL,
                                MissingGroupAggregate,
                                &member,
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

impl<M: IndexDomain, K: GroupKey, V: ValueType> GroupKernel<M, K, OperandHandle<Bare<V>, Single>>
    for BroadcastOperation
{
    type Output = OperandHandle<Indexed<M, V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Bare<V>, Single>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        Ok(Box::new(
            buckets
                .into_iter()
                .flat_map(|(_, members, payload)| {
                    let aggregate = match payload {
                        Ok(Some(outcome)) => Some(outcome),
                        Ok(None) => None,
                        Err(failure) => Some(Err(failure)),
                    };

                    members.into_iter().map(move |member| {
                        let outcome = aggregate.clone().unwrap_or_else(|| {
                            Err(Failure::new_at::<M, _>(
                                Self::LABEL,
                                MissingGroupAggregate,
                                &member,
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

impl<M: IndexDomain, K: GroupKey, I: IndexDomain, V: ValueType>
    GroupKernel<M, K, OperandHandle<Indexed<I, V>, Definite>> for BroadcastOperation
{
    type Output = OperandHandle<Indexed<M, V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Indexed<I, V>, Definite>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
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

impl<M: IndexDomain, K: GroupKey, V: ValueType> GroupKernel<M, K, OperandHandle<Bare<V>, Definite>>
    for BroadcastOperation
{
    type Output = OperandHandle<Indexed<M, V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Bare<V>, Definite>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
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

impl<O: Apply<BroadcastOperation>> Broadcast for O {
    type ReturnOperand = O::Output;

    fn broadcast(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), BroadcastOperation))
    }
}
