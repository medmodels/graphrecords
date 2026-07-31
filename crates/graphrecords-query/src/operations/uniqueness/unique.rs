use crate::{
    Bare, BareValueDomain, EvaluateOperand, Explain, Multiple, Operand, OrderState, QueryResult,
    capabilities::ValueEquivalence,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{Apply, BareStream, LaneKernel, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Unique,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Unique")]
#[plan(optimizer_hints(empty = if_any))]
pub struct UniqueOperation;

impl Prepare for UniqueOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<V: ValueEquivalence + BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for UniqueOperation
{
    type Output = OperandHandle<Bare<V>, Multiple<O>>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
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

impl<O: Apply<UniqueOperation>> Unique for O {
    type ReturnOperand = O::Output;

    fn unique(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), UniqueOperation))
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
            output: OperandHandle<Bare<V>, Multiple<O>>;
        }
    }
}
