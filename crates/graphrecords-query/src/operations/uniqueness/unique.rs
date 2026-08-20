use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, Multiple, OrderState, QueryResult,
    capabilities::ValueEquivalence,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Unique,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Unique")]
#[plan(optimizer_hints(empty = if_any))]
pub struct UniqueOperation;

impl<V: ValueEquivalence + BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for UniqueOperation
{
    type Output = ExpressionHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut seen = GrHashSet::default();

        Ok(Box::new(values.filter_map(move |outcome| match outcome {
            Ok(value) if seen.insert(V::equivalence_key(&value)) => Some(Ok(value)),
            Ok(_) => None,
            Err(failure) => Some(Err(failure)),
        })))
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

impl<E: Build<UniqueOperation>> Unique for E {
    type Output = E::Output;

    fn unique(&self) -> Self::Output {
        self.build(UniqueOperation)
    }
}

operation_manifest! {
    UniqueOperation {
        method: Unique::unique;
        scope: lane;

        kernel {
            parameters: <
                V: ValueEquivalence + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Multiple<O>>;
        }
    }
}
