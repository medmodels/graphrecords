use crate::{
    EvaluateExpression, Explain, Expression, IndexDomain, Labeled, Mask, QueryResult,
    element::Retention,
    expressions::{BucketChange, GroupedExpression, Partition},
    operations::{ArgumentSource, Build, GroupKernel, Keyed, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Having,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Group)]
#[explain(label = "Having")]
#[plan(optimizer_hints(empty = if_all))]
pub struct HavingOperation<P> {
    #[argument]
    predicate: P,
}

impl<M, K, E, P> GroupKernel<M, K, E> for HavingOperation<P>
where
    M: IndexDomain,
    K: IndexDomain,
    E: Expression,
    P: ArgumentSource<Keyed<K>, Mask>,
{
    type Output = GroupedExpression<M, K, E>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, E>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(partition.change_buckets(|bucket| {
            if bucket.payload().is_err() {
                return None;
            }

            let address = match K::resolve(graphrecord, bucket.key(), Self::LABEL) {
                Ok(address) => address,
                Err(failure) => {
                    return Some(BucketChange::ReplacePayload(Err(failure)));
                }
            };
            let step = P::resolve(graphrecord, &prepared, &address, Self::LABEL);

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

impl<E, P> Having<P> for E
where
    E: Build<HavingOperation<P>>,
    HavingOperation<P>: Operation,
{
    type Output = E::Output;

    fn having(&self, predicate: P) -> Self::Output {
        self.build(HavingOperation { predicate })
    }
}

operation_manifest! {
    HavingOperation<P> {
        method: Having<P>::having;
        scope: group;

        kernel {
            group: <M: IndexDomain, K: IndexDomain>;
            parameters: <E: Lane>;
            argument: P: ArgumentSource<Keyed<K>, Mask>;
            input: E;
            output: GroupedExpression<M, K, E>;
        }
    }
}
