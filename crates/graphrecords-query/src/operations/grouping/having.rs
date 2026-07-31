use crate::{
    EvaluateOperand, Explain, IndexDomain, Labeled, Mask, Operand, QueryResult,
    element::Retention,
    execution::EvaluationCache,
    index::GroupKey,
    operands::{BucketChange, GroupOperand, Partition},
    operations::{Apply, ArgumentSource, GroupKernel, Keyed, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Having,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Group)]
#[explain(label = "Having")]
#[plan(optimizer_hints(empty = if_all))]
pub struct HavingOperation<P> {
    #[argument]
    predicate: P,
}

impl<P: Prepare> Prepare for HavingOperation<P> {
    type Prepared<'a>
        = P::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.predicate.prepare(graphrecord, cache)
    }
}

impl<M, K, O, P> GroupKernel<M, K, O> for HavingOperation<P>
where
    M: IndexDomain,
    K: GroupKey,
    O: Operand,
    P: ArgumentSource<Keyed<K>, Mask>,
{
    type Output = GroupOperand<M, K, O>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, O>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(partition.change_buckets(|bucket| {
            if bucket.payload().is_err() {
                return None;
            }

            let key = match K::resolve_key(Self::LABEL, graphrecord, bucket.key()) {
                Ok(key) => key,
                Err(failure) => {
                    return Some(BucketChange::ReplacePayload(Err(failure)));
                }
            };
            let step = P::resolve(&prepared, &key, Self::LABEL);

            match P::Retention::collapse(step) {
                None | Some(Ok(false)) => Some(BucketChange::Drop),
                Some(Ok(true)) => None,
                Some(Err(failure)) => Some(BucketChange::ReplacePayload(Err(failure))),
            }
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            per_group: input.per_group,
            ..Estimate::UNKNOWN
        }
    }
}

impl<O, P> Having<P> for O
where
    O: Apply<HavingOperation<P>>,
    HavingOperation<P>: Operation,
{
    type ReturnOperand = O::Output;

    fn having(&self, predicate: P) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            HavingOperation { predicate },
        ))
    }
}

operation_manifest! {
    HavingOperation<P> {
        method: Having<P>::having;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: GroupKey>;
            parameters: <O: Lane>;
            argument: P: ArgumentSource<Keyed<K>, Mask>;
            input: O;
            output: GroupOperand<M, K, O>;
        }
    }
}
