use crate::{
    Bare, EvaluateExpression, Explain, IndexDomain, Indexed, Mask, Multiple, OrderState,
    QueryResult,
    expressions::DefiniteBareBoolExpression,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Any,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Any")]
pub struct AnyOperation;

fn any(values: impl Iterator<Item = QueryResult<bool>>) -> QueryResult<bool> {
    for value in values {
        if value? {
            return Ok(true);
        }
    }

    Ok(false)
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, Mask>, Multiple<O>> for AnyOperation {
    type Output = DefiniteBareBoolExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(any(values.map(|(_, value)| value)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: OrderState> LaneKernel<Bare<Mask>, Multiple<O>> for AnyOperation {
    type Output = DefiniteBareBoolExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(any(values))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<E: Build<AnyOperation>> Any for E {
    type Output = E::Output;

    fn any(&self) -> Self::Output {
        self.build(AnyOperation)
    }
}

operation_manifest! {
    AnyOperation {
        method: Any::any;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                O: OrderState,
            >;
            input: (Indexed<I, Mask>, Multiple<O>);
            output: DefiniteBareBoolExpression;
        }

        kernel {
            parameters: <O: OrderState>;
            input: (Bare<Mask>, Multiple<O>);
            output: DefiniteBareBoolExpression;
        }
    }
}
