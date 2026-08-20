use crate::{
    Definite, EvaluateExpression, Explain, IndexDomain, Indexed, Labeled, Multiple, QueryResult,
    Single, ValueDomain,
    capabilities::ValueGrouping,
    element::Retention,
    expressions::{
        ExpressionHandle, GroupedExpression, PartitionArity, PartitionBuilder,
        PartitionClassification,
    },
    operations::{ArgumentSource, Build, Keyed, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::GroupBy,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "GroupBy")]
#[plan(optimizer_hints(empty = if_all))]
pub struct GroupByOperation<K> {
    #[argument]
    key: K,
}

impl<I, V, K, C> LaneKernel<Indexed<I, V>, C> for GroupByOperation<K>
where
    I: IndexDomain,
    V: ValueDomain,
    K: ArgumentSource<Keyed<I>>,
    K::ValueDomain: ValueGrouping,
    C: PartitionArity<Indexed<I, V>>,
{
    type Output = GroupedExpression<
        I,
        <K::ValueDomain as ValueGrouping>::KeyDomain,
        ExpressionHandle<Indexed<I, V>, C>,
    >;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, C>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let label = Self::LABEL;

        PartitionBuilder::<_, _, _, C>::new(values).build(graphrecord, |element| {
            let member = &element.0;
            let step = K::resolve(graphrecord, &prepared, member, label);

            match K::Retention::collapse(step) {
                None => PartitionClassification::Omit,
                Some(Err(failure)) => PartitionClassification::KeyFailure(failure),
                Some(Ok(value)) => {
                    PartitionClassification::Key(K::ValueDomain::to_group_key(&value))
                }
            }
        })
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        let (Some(elements), Some(keys)) = (input.elements, self.key.estimate(stats).distinct)
        else {
            return Estimate {
                per_group: Some(Box::new(Estimate::UNKNOWN)),
                ..Estimate::UNKNOWN
            };
        };

        let groups = keys.min(elements);
        let per_group_elements = if groups == 0 {
            0
        } else {
            elements.div_ceil(groups)
        };

        Estimate {
            elements: Some(groups),
            distinct: Some(groups),
            selectivity: None,
            per_group: Some(Box::new(Estimate {
                elements: Some(per_group_elements),
                distinct: input
                    .distinct
                    .map(|distinct| distinct.min(per_group_elements)),
                selectivity: input.selectivity,
                per_group: None,
            })),
        }
    }
}

impl<E, K> GroupBy<K> for E
where
    E: Build<GroupByOperation<K>>,
    GroupByOperation<K>: Operation,
{
    type Output = E::Output;

    fn group_by(&self, key: K) -> Self::Output {
        self.build(GroupByOperation { key })
    }
}

operation_manifest! {
    GroupByOperation<K> {
        method: GroupBy<K>::group_by;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, X: ValueGrouping, O: OrderState>;
            argument: K: ArgumentSource<Keyed<I>, X>;
            input: (Indexed<I, V>, Multiple<O>);
            output: GroupedExpression<
                I,
                <X as ValueGrouping>::KeyDomain,
                ExpressionHandle<Indexed<I, V>, Multiple<O>>,
            >;
        }

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, X: ValueGrouping>;
            argument: K: ArgumentSource<Keyed<I>, X>;
            input: (Indexed<I, V>, Single);
            output: GroupedExpression<
                I,
                <X as ValueGrouping>::KeyDomain,
                ExpressionHandle<Indexed<I, V>, Single>,
            >;
        }

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, X: ValueGrouping>;
            argument: K: ArgumentSource<Keyed<I>, X>;
            input: (Indexed<I, V>, Definite);
            output: GroupedExpression<
                I,
                <X as ValueGrouping>::KeyDomain,
                ExpressionHandle<Indexed<I, V>, Definite>,
            >;
        }
    }
}
