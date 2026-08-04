use super::reject_key_failures;
use crate::{
    Bare, BareValueDomain, Definite, EvaluateOperand, Explain, Failure, IndexDomain, Indexed,
    Labeled, Multiple, Operand, QueryResult, Single, Unordered, ValueDomain,
    error::grouping::UnresolvedBucketFailures,
    execution::EvaluationCache,
    index::GroupKey,
    operands::{CheckedIndexedLaneBuilder, OperandHandle, Partition, PartitionArity},
    operations::{Apply, GroupKernel, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Ungroup,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
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

impl Prepare for UngroupOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<M: IndexDomain, K: GroupKey, I: IndexDomain, V: ValueDomain, C: PartitionArity<Indexed<I, V>>>
    GroupKernel<M, K, OperandHandle<Indexed<I, V>, C>> for UngroupOperation
{
    type Output = OperandHandle<Indexed<I, V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Indexed<I, V>, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let mut output = CheckedIndexedLaneBuilder::<I, V>::new();
        let mut bucket_failures = Vec::new();

        for bucket in buckets {
            match bucket.2 {
                Ok(payload) => {
                    for (index, outcome) in C::into_elements(payload) {
                        output.push(index, outcome)?;
                    }
                }
                Err(failure) => bucket_failures.push(*failure),
            }
        }

        if !bucket_failures.is_empty() {
            return Err(Failure::new(
                Self::LABEL,
                UnresolvedBucketFailures::new(bucket_failures),
            ));
        }

        Ok(output.finish())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        ungroup_estimate(&input)
    }
}

impl<M: IndexDomain, K: GroupKey, V: BareValueDomain, C: PartitionArity<Bare<V>>>
    GroupKernel<M, K, OperandHandle<Bare<V>, C>> for UngroupOperation
{
    type Output = OperandHandle<Bare<V>, Multiple<Unordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Bare<V>, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
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
                Self::LABEL,
                UnresolvedBucketFailures::new(bucket_failures),
            ));
        }

        Ok(Box::new(payloads.into_iter().flat_map(C::into_elements)))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        ungroup_estimate(&input)
    }
}

impl<O: Apply<UngroupOperation>> Ungroup for O {
    type ReturnOperand = O::Output;

    fn ungroup(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UngroupOperation))
    }
}

operation_manifest! {
    UngroupOperation {
        method: Ungroup::ungroup;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <I: IndexDomain, V: ValueDomain, O: OrderState>;
            input: OperandHandle<Indexed<I, V>, Multiple<O>>;
            output: OperandHandle<Indexed<I, V>, Multiple<Unordered>>;
        }
        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: OperandHandle<Indexed<I, V>, Single>;
            output: OperandHandle<Indexed<I, V>, Multiple<Unordered>>;
        }
        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: OperandHandle<Indexed<I, V>, Definite>;
            output: OperandHandle<Indexed<I, V>, Multiple<Unordered>>;
        }
        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <V: BareValueDomain, O: OrderState>;
            input: OperandHandle<Bare<V>, Multiple<O>>;
            output: OperandHandle<Bare<V>, Multiple<Unordered>>;
        }
        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <V: BareValueDomain>;
            input: OperandHandle<Bare<V>, Single>;
            output: OperandHandle<Bare<V>, Multiple<Unordered>>;
        }
        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <V: BareValueDomain>;
            input: OperandHandle<Bare<V>, Definite>;
            output: OperandHandle<Bare<V>, Multiple<Unordered>>;
        }
    }
}
