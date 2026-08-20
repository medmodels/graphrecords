use super::reject_key_failures;
use crate::{
    Bare, BareValueDomain, Definite, EvaluateExpression, Explain, Failure, IndexDomain, Indexed,
    Labeled, Multiple, QueryResult, Single, Unordered, ValueDomain,
    error::grouping::MissingGroupAggregate,
    expressions::{ExpressionHandle, Partition},
    operations::{Build, GroupKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::UngroupKeyed,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
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

impl<M: IndexDomain, K: IndexDomain, I: IndexDomain, V: ValueDomain>
    GroupKernel<M, K, ExpressionHandle<Indexed<I, V>, Single>> for UngroupKeyedOperation
{
    type Output = ExpressionHandle<Indexed<K, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Indexed<I, V>, Single>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let address = K::resolve(graphrecord, &key, Self::LABEL)?;
                let outcome = match payload {
                    Ok(Some((_, outcome))) => outcome,
                    Ok(None) => Err(Failure::new_at_address::<K, _>(
                        MissingGroupAggregate,
                        graphrecord,
                        &address,
                        Self::LABEL,
                    )),
                    Err(failure) => Err(failure),
                };

                Ok((address, outcome))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        bucket_element_estimate(&input)
    }
}

impl<M: IndexDomain, K: IndexDomain, I: IndexDomain, V: ValueDomain>
    GroupKernel<M, K, ExpressionHandle<Indexed<I, V>, Definite>> for UngroupKeyedOperation
{
    type Output = ExpressionHandle<Indexed<K, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Indexed<I, V>, Definite>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let address = K::resolve(graphrecord, &key, Self::LABEL)?;
                let outcome = match payload {
                    Ok((_, outcome)) => outcome,
                    Err(failure) => Err(failure),
                };

                Ok((address, outcome))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        bucket_element_estimate(&input)
    }
}

impl<M: IndexDomain, K: IndexDomain, V: BareValueDomain>
    GroupKernel<M, K, ExpressionHandle<Bare<V>, Single>> for UngroupKeyedOperation
{
    type Output = ExpressionHandle<Indexed<K, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Bare<V>, Single>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let address = K::resolve(graphrecord, &key, Self::LABEL)?;
                let outcome = match payload {
                    Ok(Some(outcome)) => outcome,
                    Ok(None) => Err(Failure::new_at_address::<K, _>(
                        MissingGroupAggregate,
                        graphrecord,
                        &address,
                        Self::LABEL,
                    )),
                    Err(failure) => Err(failure),
                };

                Ok((address, outcome))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        bucket_element_estimate(&input)
    }
}

impl<M: IndexDomain, K: IndexDomain, V: BareValueDomain>
    GroupKernel<M, K, ExpressionHandle<Bare<V>, Definite>> for UngroupKeyedOperation
{
    type Output = ExpressionHandle<Indexed<K, V>, Multiple<Unordered>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<Bare<V>, Definite>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let address = K::resolve(graphrecord, &key, Self::LABEL)?;
                let outcome = match payload {
                    Ok(outcome) => outcome,
                    Err(failure) => Err(failure),
                };

                Ok((address, outcome))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        bucket_element_estimate(&input)
    }
}

impl<E: Build<UngroupKeyedOperation>> UngroupKeyed for E {
    type Output = E::Output;

    fn ungroup_keyed(&self) -> Self::Output {
        self.build(UngroupKeyedOperation)
    }
}

operation_manifest! {
    UngroupKeyedOperation {
        method: UngroupKeyed::ungroup_keyed;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: ExpressionHandle<Indexed<I, V>, Single>;
            output: ExpressionHandle<Indexed<K, V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <I: IndexDomain, V: ValueDomain>;
            input: ExpressionHandle<Indexed<I, V>, Definite>;
            output: ExpressionHandle<Indexed<K, V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain>;
            input: ExpressionHandle<Bare<V>, Single>;
            output: ExpressionHandle<Indexed<K, V>, Multiple<Unordered>>;
        }

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <V: BareValueDomain>;
            input: ExpressionHandle<Bare<V>, Definite>;
            output: ExpressionHandle<Indexed<K, V>, Multiple<Unordered>>;
        }
    }
}
