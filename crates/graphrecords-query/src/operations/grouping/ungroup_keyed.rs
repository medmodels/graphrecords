use super::reject_key_failures;
use crate::{
    Bare, Definite, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple,
    Operand, QueryResult, Single, Unordered, ValueType,
    error::grouping::MissingGroupAggregate,
    execution::EvaluationCache,
    index::GroupKey,
    operands::{OperandHandle, Partition},
    operations::{Apply, GroupKernel, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::UngroupKeyed,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "UngroupKeyed")]
pub struct UngroupKeyedOperation;

fn single_estimate(input: &Estimate) -> Estimate {
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

impl<M: IndexDomain, K: GroupKey, I: IndexDomain, V: ValueType>
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
        single_estimate(&input)
    }
}

impl<M: IndexDomain, K: GroupKey, V: ValueType> GroupKernel<M, K, OperandHandle<Bare<V>, Single>>
    for UngroupKeyedOperation
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
        single_estimate(&input)
    }
}

impl<M: IndexDomain, K: GroupKey, I: IndexDomain, V: ValueType>
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
        single_estimate(&input)
    }
}

impl<M: IndexDomain, K: GroupKey, V: ValueType> GroupKernel<M, K, OperandHandle<Bare<V>, Definite>>
    for UngroupKeyedOperation
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
        single_estimate(&input)
    }
}

impl<O: Apply<UngroupKeyedOperation>> UngroupKeyed for O {
    type ReturnOperand = O::Output;

    fn ungroup_keyed(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UngroupKeyedOperation))
    }
}
