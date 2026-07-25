use crate::{
    Bare, Definite, EvaluateOperand, Explain, IndexDomain, Indexed, Multiple, Operand, OrderState,
    QueryResult, Single, Unordered, ValueType,
    execution::EvaluationCache,
    operands::{ElementsOperand, GroupOperand, OperandHandle},
    operations::{Apply, KeyOperand, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Keys,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Keys")]
pub struct KeysOperation;

const fn keys_estimate(input: &Estimate) -> Estimate {
    Estimate {
        elements: input.elements,
        distinct: input.elements,
        selectivity: None,
        per_group: None,
    }
}

impl Prepare for KeysOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V, O, K> Apply<KeysOperation> for GroupOperand<OperandHandle<Indexed<I, V>, Multiple<O>>, K>
where
    I: IndexDomain,
    V: ValueType,
    O: OrderState,
    K: KeyOperand,
{
    type Output = ElementsOperand<K::Key, Unordered>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <KeysOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(
            values.map(|(key, values)| (key, values.map(|_values| ()))),
        ))
    }

    fn estimate(_operation: &KeysOperation, input: Estimate, _stats: &Stats) -> Estimate {
        keys_estimate(&input)
    }
}

impl<V, O, K> Apply<KeysOperation> for GroupOperand<OperandHandle<Bare<V>, Multiple<O>>, K>
where
    V: ValueType,
    O: OrderState,
    K: KeyOperand,
{
    type Output = ElementsOperand<K::Key, Unordered>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <KeysOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(
            values.map(|(key, values)| (key, values.map(|_values| ()))),
        ))
    }

    fn estimate(_operation: &KeysOperation, input: Estimate, _stats: &Stats) -> Estimate {
        keys_estimate(&input)
    }
}

impl<I, V, K> Apply<KeysOperation> for GroupOperand<OperandHandle<Indexed<I, V>, Single>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Output = ElementsOperand<K::Key, Unordered>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <KeysOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(values.map(|(key, value)| {
            let value = match value {
                Ok(Some((_index, value))) => value.map(|_value| ()),
                Ok(None) => Ok(()),
                Err(failure) => Err(failure),
            };

            (key, value)
        })))
    }

    fn estimate(_operation: &KeysOperation, input: Estimate, _stats: &Stats) -> Estimate {
        keys_estimate(&input)
    }
}

impl<V, K> Apply<KeysOperation> for GroupOperand<OperandHandle<Bare<V>, Single>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Output = ElementsOperand<K::Key, Unordered>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <KeysOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(values.map(|(key, value)| {
            let value = match value {
                Ok(Some(value)) => value.map(|_value| ()),
                Ok(None) => Ok(()),
                Err(failure) => Err(failure),
            };

            (key, value)
        })))
    }

    fn estimate(_operation: &KeysOperation, input: Estimate, _stats: &Stats) -> Estimate {
        keys_estimate(&input)
    }
}

impl<I, V, K> Apply<KeysOperation> for GroupOperand<OperandHandle<Indexed<I, V>, Definite>, K>
where
    I: IndexDomain,
    V: ValueType,
    K: KeyOperand,
{
    type Output = ElementsOperand<K::Key, Unordered>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <KeysOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(values.map(|(key, value)| {
            let value = value.and_then(|(_index, value)| value.map(|_value| ()));

            (key, value)
        })))
    }

    fn estimate(_operation: &KeysOperation, input: Estimate, _stats: &Stats) -> Estimate {
        keys_estimate(&input)
    }
}

impl<V, K> Apply<KeysOperation> for GroupOperand<OperandHandle<Bare<V>, Definite>, K>
where
    V: ValueType,
    K: KeyOperand,
{
    type Output = ElementsOperand<K::Key, Unordered>;

    fn apply<'a>(
        _graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        _prepared: <KeysOperation as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(Box::new(values.map(|(key, value)| {
            let value = value.and_then(|value| value.map(|_value| ()));

            (key, value)
        })))
    }

    fn estimate(_operation: &KeysOperation, input: Estimate, _stats: &Stats) -> Estimate {
        keys_estimate(&input)
    }
}

impl<O> Keys for O
where
    O: Apply<KeysOperation>,
{
    type ReturnOperand = <O as Apply<KeysOperation>>::Output;

    fn keys(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), KeysOperation))
    }
}
