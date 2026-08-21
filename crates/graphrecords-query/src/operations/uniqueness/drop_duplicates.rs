use crate::{
    EvaluateExpression, Explain, IndexDomain, Indexed, Multiple, Ordered, QueryResult,
    capabilities::ValueEquivalence,
    expressions::ExpressionHandle,
    operations::{Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::DropDuplicates,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "DropDuplicates")]
#[plan(optimizer_hints(empty = if_any))]
pub struct DropDuplicatesOperation;

impl<I: IndexDomain, V: ValueEquivalence> LaneKernel<Indexed<I, V>, Multiple<Ordered>>
    for DropDuplicatesOperation
{
    type Output = ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, V, Multiple<Ordered>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut seen = GrHashSet::default();

        Ok(Box::new(values.filter_map(
            move |(index, outcome)| match outcome {
                Ok(value) if seen.insert(V::equivalence_key(&value)) => Some((index, Ok(value))),
                Ok(_) => None,
                Err(failure) => Some((index, Err(failure))),
            },
        )))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            elements: None,
            distinct: input.distinct,
            selectivity: None,
            per_group: None,
        }
    }
}

impl<E: Build<DropDuplicatesOperation>> DropDuplicates for E {
    type Output = E::Output;

    fn drop_duplicates(&self) -> Self::Output {
        self.build(DropDuplicatesOperation)
    }
}

operation_manifest! {
    DropDuplicatesOperation {
        method: DropDuplicates::drop_duplicates;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueEquivalence,
            >;
            input: (Indexed<I, V>, Multiple<Ordered>);
            output: ExpressionHandle<Indexed<I, V>, Multiple<Ordered>>;
        }
    }
}
