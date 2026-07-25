use crate::{
    Bare, Definite, EvaluateOperand, Explain, Failure, IndexDomain, Indexed, Labeled, Multiple,
    Operand, QueryResult, Single, Unordered, ValueType,
    execution::EvaluationCache,
    operands::{GroupOperand, OperandHandle},
    operations::{Apply, KeyOperand, MissingGroupAggregate, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::UngroupKeyed,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
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

impl<I, V, K> Apply<UngroupKeyedOperation> for GroupOperand<OperandHandle<Indexed<I, V>, Single>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Output = OperandHandle<Indexed<K::Key, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupKeyedOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let label = UngroupKeyedOperation::LABEL;

        Ok(Box::new(values.map(move |(key, value)| {
            let value = match value {
                Ok(Some((_index, value))) => value,
                Ok(None) => Err(Failure::new_at(label, MissingGroupAggregate, &key)),
                Err(failure) => Err(failure),
            };

            (key, value)
        })))
    }

    fn estimate(_operation: &UngroupKeyedOperation, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<V, K> Apply<UngroupKeyedOperation> for GroupOperand<OperandHandle<Bare<V>, Single>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Output = OperandHandle<Indexed<K::Key, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupKeyedOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let label = UngroupKeyedOperation::LABEL;

        Ok(Box::new(values.map(move |(key, value)| {
            let value = match value {
                Ok(Some(value)) => value,
                Ok(None) => Err(Failure::new_at(label, MissingGroupAggregate, &key)),
                Err(failure) => Err(failure),
            };

            (key, value)
        })))
    }

    fn estimate(_operation: &UngroupKeyedOperation, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<I, V, K> Apply<UngroupKeyedOperation>
    for GroupOperand<OperandHandle<Indexed<I, V>, Definite>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Output = OperandHandle<Indexed<K::Key, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupKeyedOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(values.map(|(key, value)| {
            let value = value.and_then(|(_index, value)| value);

            (key, value)
        })))
    }

    fn estimate(_operation: &UngroupKeyedOperation, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<V, K> Apply<UngroupKeyedOperation> for GroupOperand<OperandHandle<Bare<V>, Definite>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Output = OperandHandle<Indexed<K::Key, V>, Multiple<Unordered>>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <UngroupKeyedOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(
            values.map(|(key, value)| (key, value.and_then(|value| value))),
        ))
    }

    fn estimate(_operation: &UngroupKeyedOperation, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<O> UngroupKeyed for O
where
    O: Apply<UngroupKeyedOperation>,
{
    type ReturnOperand = <O as Apply<UngroupKeyedOperation>>::Output;

    fn ungroup_keyed(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UngroupKeyedOperation))
    }
}
