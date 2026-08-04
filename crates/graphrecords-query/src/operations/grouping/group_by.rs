use crate::{
    Definite, EvaluateOperand, Explain, IndexDomain, Indexed, Labeled, Multiple, Operand,
    QueryResult, Single, ValueDomain,
    capabilities::GroupingValue,
    element::Retention,
    execution::EvaluationCache,
    operands::{
        GroupOperand, OperandHandle, PartitionArity, PartitionBuilder, PartitionClassification,
    },
    operations::{
        Apply, ArgumentSource, Keyed, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::GroupBy,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "GroupBy")]
pub struct GroupByOperation<K> {
    #[argument]
    key: K,
}

impl<K: Prepare> Prepare for GroupByOperation<K> {
    type Prepared<'a>
        = K::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.key.prepare(graphrecord, cache)
    }
}

impl<I, V, K, C> LaneKernel<Indexed<I, V>, C> for GroupByOperation<K>
where
    I: IndexDomain,
    V: ValueDomain,
    K: ArgumentSource<Keyed<I>>,
    K::ValueDomain: GroupingValue,
    C: PartitionArity<Indexed<I, V>>,
{
    type Output =
        GroupOperand<I, <K::ValueDomain as GroupingValue>::Key, OperandHandle<Indexed<I, V>, C>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, C>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let label = Self::LABEL;

        PartitionBuilder::<_, _, _, C>::new(values).build(|element| {
            let member = &element.0;
            let step = K::resolve(&prepared, member, label);

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

impl<O, K> GroupBy<K> for O
where
    O: Apply<GroupByOperation<K>>,
    GroupByOperation<K>: Operation,
{
    type ReturnOperand = O::Output;

    fn group_by(&self, key: K) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            GroupByOperation { key },
        ))
    }
}

operation_manifest! {
    GroupByOperation<K> {
        method: GroupBy<K>::group_by;
        scope: lane;

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, X: GroupingValue, O: OrderState>;
            argument: K: ArgumentSource<Keyed<I>, X>;
            input: (Indexed<I, V>, Multiple<O>);
            output: GroupOperand<
                I,
                <X as GroupingValue>::Key,
                OperandHandle<Indexed<I, V>, Multiple<O>>,
            >;
        }

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, X: GroupingValue>;
            argument: K: ArgumentSource<Keyed<I>, X>;
            input: (Indexed<I, V>, Single);
            output: GroupOperand<
                I,
                <X as GroupingValue>::Key,
                OperandHandle<Indexed<I, V>, Single>,
            >;
        }

        kernel {
            parameters: <I: IndexDomain, V: ValueDomain, X: GroupingValue>;
            argument: K: ArgumentSource<Keyed<I>, X>;
            input: (Indexed<I, V>, Definite);
            output: GroupOperand<
                I,
                <X as GroupingValue>::Key,
                OperandHandle<Indexed<I, V>, Definite>,
            >;
        }
    }
}
