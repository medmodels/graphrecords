use super::reject_key_failures;
use crate::{
    Bare, BareValueDomain, Definite, EvaluateOperand, Explain, Failure, IndexDomain, Indexed,
    Labeled, Multiple, Operand, QueryResult, Single, Unordered, ValueDomain,
    error::grouping::MissingGroupAggregate,
    execution::EvaluationCache,
    index::GroupKey,
    operands::{OperandHandle, Partition},
    operations::{Apply, GroupKernel, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::UngroupKeyed,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "UngroupKeyed")]
#[plan(optimizer_hints(empty = if_any))]
pub struct UngroupKeyedOperation;

fn bucket_element_estimate(input: &Estimate) -> Estimate {
    Estimate {
        elements: input.elements,
        distinct: input.elements,
        selectivity: input
            .per_group
            .as_deref()
            .and_then(|inner| inner.selectivity),
        per_group: None,
    }
}

impl Prepare for UngroupKeyedOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<M: IndexDomain, K: GroupKey, I: IndexDomain, V: ValueDomain>
    GroupKernel<M, K, OperandHandle<Indexed<I, V>, Single>> for UngroupKeyedOperation
{
    type Output = OperandHandle<Indexed<K, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Indexed<I, V>, Single>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let index = K::resolve_key(Self::LABEL, graphrecord, &key)?;
                let outcome = match payload {
                    Ok(Some((_, outcome))) => outcome,
                    Ok(None) => Err(Failure::new_at::<K, _>(
                        Self::LABEL,
                        MissingGroupAggregate,
                        &index,
                    )),
                    Err(failure) => Err(failure),
                };

                Ok((index, outcome))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        bucket_element_estimate(&input)
    }
}

impl<M: IndexDomain, K: GroupKey, V: BareValueDomain>
    GroupKernel<M, K, OperandHandle<Bare<V>, Single>> for UngroupKeyedOperation
{
    type Output = OperandHandle<Indexed<K, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Bare<V>, Single>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let index = K::resolve_key(Self::LABEL, graphrecord, &key)?;
                let outcome = match payload {
                    Ok(Some(outcome)) => outcome,
                    Ok(None) => Err(Failure::new_at::<K, _>(
                        Self::LABEL,
                        MissingGroupAggregate,
                        &index,
                    )),
                    Err(failure) => Err(failure),
                };

                Ok((index, outcome))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        bucket_element_estimate(&input)
    }
}

impl<M: IndexDomain, K: GroupKey, I: IndexDomain, V: ValueDomain>
    GroupKernel<M, K, OperandHandle<Indexed<I, V>, Definite>> for UngroupKeyedOperation
{
    type Output = OperandHandle<Indexed<K, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Indexed<I, V>, Definite>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let index = K::resolve_key(Self::LABEL, graphrecord, &key)?;
                let outcome = match payload {
                    Ok((_, outcome)) => outcome,
                    Err(failure) => Err(failure),
                };

                Ok((index, outcome))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        bucket_element_estimate(&input)
    }
}

impl<M: IndexDomain, K: GroupKey, V: BareValueDomain>
    GroupKernel<M, K, OperandHandle<Bare<V>, Definite>> for UngroupKeyedOperation
{
    type Output = OperandHandle<Indexed<K, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<Bare<V>, Definite>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let index = K::resolve_key(Self::LABEL, graphrecord, &key)?;
                let outcome = match payload {
                    Ok(outcome) => outcome,
                    Err(failure) => Err(failure),
                };

                Ok((index, outcome))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        bucket_element_estimate(&input)
    }
}

impl<O: Apply<UngroupKeyedOperation>> UngroupKeyed for O {
    type ReturnOperand = O::Output;

    fn ungroup_keyed(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UngroupKeyedOperation))
    }
}

operation_manifest! {
    UngroupKeyedOperation {
        method: UngroupKeyed::ungroup_keyed;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: OperandHandle<Indexed<I, V>, Single>;
            output: OperandHandle<Indexed<K, V>, Multiple<Unordered>>;
        }
        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <V: BareValueDomain>;
            input: OperandHandle<Bare<V>, Single>;
            output: OperandHandle<Indexed<K, V>, Multiple<Unordered>>;
        }
        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: OperandHandle<Indexed<I, V>, Definite>;
            output: OperandHandle<Indexed<K, V>, Multiple<Unordered>>;
        }
        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <V: BareValueDomain>;
            input: OperandHandle<Bare<V>, Definite>;
            output: OperandHandle<Indexed<K, V>, Multiple<Unordered>>;
        }
    }
}
