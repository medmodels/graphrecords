use super::reject_key_failures;
use crate::{
    Bare, BareValueDomain, Definite, EvaluateExpression, Explain, Failure, IndexDomain, Indexed,
    Labeled, Multiple, QueryResult, Single, Unordered, ValueDomain,
    error::grouping::UnresolvedBucketFailures,
    expressions::{CheckedIndexedLaneBuilder, ExpressionHandle, Partition, PartitionArity},
    operations::{Build, GroupKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Ungroup,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Group)]
#[explain(label = "Ungroup")]
#[plan(optimizer_hints(empty = if_any))]
pub struct UngroupOperation;

fn ungroup_estimate(input: &Estimate) -> Estimate {
    let per_group = input.per_group.as_deref();
    let elements = match (input.elements, per_group.and_then(|inner| inner.elements)) {
        (Some(groups), Some(elements_per_group)) => groups.checked_mul(elements_per_group),
        _ => None,
    };
    let distinct = match (input.elements, per_group.and_then(|inner| inner.distinct)) {
        (Some(groups), Some(distinct_per_group)) => groups.checked_mul(distinct_per_group),
        _ => None,
    };

    Estimate {
        elements,
        distinct: match (distinct, elements) {
            (Some(distinct), Some(elements)) => Some(distinct.min(elements)),
            (distinct, _) => distinct,
        },
        selectivity: per_group.and_then(|inner| inner.selectivity),
        per_group: None,
    }
}

impl<
    M: IndexDomain,
    K: IndexDomain,
    I: IndexDomain,
    V: ValueDomain,
    C: PartitionArity<Indexed<I, V>>,
> GroupKernel<M, K, ExpressionHandle<Indexed<I, V>, C>> for UngroupOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Indexed<I, V>, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let mut output = CheckedIndexedLaneBuilder::<I, V>::new();
        let mut bucket_failures = Vec::new();

        for bucket in buckets {
            match bucket.2 {
                Ok(payload) => {
                    for (address, outcome) in C::into_elements(payload) {
                        output.push(graphrecord, address, outcome)?;
                    }
                }
                Err(failure) => bucket_failures.push(*failure),
            }
        }

        if !bucket_failures.is_empty() {
            return Err(Failure::new(
                UnresolvedBucketFailures::new(bucket_failures),
                Self::LABEL,
            ));
        }

        Ok(output.finish())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        ungroup_estimate(&input)
    }
}

impl<M: IndexDomain, K: IndexDomain, V: BareValueDomain, C: PartitionArity<Bare<V>>>
    GroupKernel<M, K, ExpressionHandle<Bare<V>, C>> for UngroupOperation
{
    type Output = ExpressionHandle<Bare<V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Bare<V>, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let mut payloads = Vec::with_capacity(buckets.len());
        let mut bucket_failures = Vec::new();

        for bucket in buckets {
            match bucket.2 {
                Ok(payload) => payloads.push(payload),
                Err(failure) => bucket_failures.push(*failure),
            }
        }

        if !bucket_failures.is_empty() {
            return Err(Failure::new(
                UnresolvedBucketFailures::new(bucket_failures),
                Self::LABEL,
            ));
        }

        Ok(Box::new(payloads.into_iter().flat_map(C::into_elements)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        ungroup_estimate(&input)
    }
}

impl<E: Build<UngroupOperation>> Ungroup for E {
    type Output = E::Output;

    fn ungroup(&self) -> Self::Output {
        self.build(UngroupOperation)
    }
}

operation_manifest! {
    UngroupOperation {
        method: Ungroup::ungroup;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            input: ExpressionHandle<Indexed<I, V>, Multiple<O>>;
            output: ExpressionHandle<Indexed<I, V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: ExpressionHandle<Indexed<I, V>, Single>;
            output: ExpressionHandle<Indexed<I, V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: ExpressionHandle<Indexed<I, V>, Definite>;
            output: ExpressionHandle<Indexed<I, V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain, O: OrderState>;
            input: ExpressionHandle<Bare<V>, Multiple<O>>;
            output: ExpressionHandle<Bare<V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain>;
            input: ExpressionHandle<Bare<V>, Single>;
            output: ExpressionHandle<Bare<V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain>;
            input: ExpressionHandle<Bare<V>, Definite>;
            output: ExpressionHandle<Bare<V>, Multiple<Unordered>>;
        }
    }
}
