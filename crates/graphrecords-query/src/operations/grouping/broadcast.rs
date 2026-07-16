use crate::{
    Diagnostic, EvaluateOperand, Explain, Failure, IndexDomain, Labeled, Operand, QueryResult,
    Unordered,
    execution::EvaluationCache,
    operands::{GroupOperand, ValueOperand, ValuesOperand},
    operations::{Apply, KeyOperand, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Broadcast,
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use graphrecords_utils::aliases::GrHashMap;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct MissingGroupAggregate;

impl Display for MissingGroupAggregate {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("no aggregate value for the element's group")
    }
}

impl Error for MissingGroupAggregate {}

impl Diagnostic for MissingGroupAggregate {
    fn help(&self) -> Option<String> {
        Some(
            "compute the aggregate over every group being broadcast to or handle the gaps with `on_error(...)`"
                .to_string(),
        )
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Broadcast")]
pub struct BroadcastOperation<K: KeyOperand> {
    #[argument]
    pub key: K,
}

impl<K: KeyOperand> Prepare for BroadcastOperation<K> {
    type Prepared<'a> = K::Prepared<'a>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Prepare::prepare(&self.key, graphrecord, cache)
    }
}

impl<I, K> Apply<BroadcastOperation<K>> for GroupOperand<ValueOperand<I>, K>
where
    I: IndexDomain,
    K: KeyOperand<Subject = I> + Operand,
{
    type Output = ValuesOperand<I, Unordered>;

    fn estimate(operation: &BroadcastOperation<K>, input: Estimate, stats: &Stats) -> Estimate {
        let elements = operation.key.estimate(stats).elements;
        let distinct = match (input.elements, elements) {
            (Some(distinct), Some(elements)) => Some(distinct.min(elements)),
            (distinct, _) => distinct,
        };

        Estimate {
            elements,
            distinct,
            selectivity: None,
            per_group: None,
        }
    }

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        groups: Self::ReturnValue<'a>,
        keys: <BroadcastOperation<K> as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let aggregates: GrHashMap<K::Key<'a>, QueryResult<GraphRecordValue>> = groups
            .filter_map(|(key, aggregate)| match aggregate {
                Ok(Some((_, value))) => Some((key, value)),
                Ok(None) => None,
                Err(failure) => Some((key, Err(failure))),
            })
            .collect();

        let assignments: Vec<_> = K::assignments(&keys).collect();

        let label = <BroadcastOperation<K> as Labeled>::LABEL;

        Ok(Box::new(assignments.into_iter().map(
            move |(index, key)| {
                if let Some(aggregate) = aggregates.get(&key) {
                    (index, aggregate.clone())
                } else {
                    let failure = Failure::new_at(label, MissingGroupAggregate, &index);

                    (index, Err(failure))
                }
            },
        )))
    }
}

impl<I, K> Broadcast<K> for GroupOperand<ValueOperand<I>, K>
where
    I: IndexDomain,
    K: KeyOperand<Subject = I> + Operand,
{
    type ReturnOperand = ValuesOperand<I, Unordered>;

    fn broadcast(&self, key: K) -> Self::ReturnOperand {
        ValuesOperand::new(OperationContext::new(
            self.clone(),
            BroadcastOperation { key },
        ))
    }
}
