use super::reject_key_failures;
use crate::{
    Arity, ElementShape, EvaluateExpression, Explain, IndexDomain, Labeled, QueryResult, Unordered,
    expressions::{ElementsExpression, ExpressionHandle, Partition},
    operations::{Build, GroupKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Keys,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Group)]
#[explain(label = "Keys")]
#[plan(optimizer_hints(empty = if_any))]
pub struct KeysOperation;

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, C: Arity>
    GroupKernel<M, K, ExpressionHandle<S, C>> for KeysOperation
{
    type Output = ElementsExpression<K, Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, ExpressionHandle<S, C>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (buckets, key_failures) = partition.into_parts();

        reject_key_failures::<M>(key_failures, Self::LABEL)?;

        let elements: Vec<_> = buckets
            .into_iter()
            .map(|(key, _, payload)| {
                let address = K::resolve(graphrecord, &key, Self::LABEL)?;

                Ok((address, payload.map(|_| ())))
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

impl<E: Build<KeysOperation>> Keys for E {
    type Output = E::Output;

    fn keys(&self) -> Self::Output {
        self.build(KeysOperation)
    }
}

operation_manifest! {
    KeysOperation {
        method: Keys::keys;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <S: ElementShape, C: Arity>;
            input: ExpressionHandle<S, C>;
            output: ElementsExpression<K, Unordered>;
        }
    }
}
