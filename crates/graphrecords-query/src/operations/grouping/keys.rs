use super::reject_key_failures;
use crate::{
    Arity, ElementShape, EvaluateOperand, Explain, IndexDomain, Labeled, Operand, QueryResult,
    Unordered,
    execution::EvaluationCache,
    index::GroupKey,
    operands::{ElementsOperand, OperandHandle, Partition},
    operations::{Apply, GroupKernel, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Keys,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "Keys")]
pub struct KeysOperation;

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

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: Arity> GroupKernel<M, K, OperandHandle<S, C>>
    for KeysOperation
{
    type Output = ElementsOperand<K, Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, OperandHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let index = K::resolve_key(Self::LABEL, graphrecord, &key)?;

                Ok((index, payload.map(|_| ())))
            })
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(elements.into_iter()))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: input.elements,
            distinct: input.elements,
            selectivity: None,
            per_group: None,
        }
    }
}

impl<O: Apply<KeysOperation>> Keys for O {
    type ReturnOperand = O::Output;

    fn keys(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), KeysOperation))
    }
}
