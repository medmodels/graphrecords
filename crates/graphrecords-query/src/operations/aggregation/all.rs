use crate::{
    Bare, EvaluateExpression, Explain, IndexDomain, Indexed, Mask, Multiple, OrderState,
    QueryResult,
    expressions::DefiniteBareBoolExpression,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::All,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "All")]
pub struct AllOperation;

fn all(values: impl Iterator<Item = QueryResult<bool>>) -> QueryResult<bool> {
    for value in values {
        if !value? {
            return Ok(false);
        }
    }

    Ok(true)
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, Mask>, Multiple<O>> for AllOperation {
    type Output = DefiniteBareBoolExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(all(values.map(|(_, value)| value)))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: OrderState> LaneKernel<Bare<Mask>, Multiple<O>> for AllOperation {
    type Output = DefiniteBareBoolExpression;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, Mask, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(all(values))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<E: Build<AllOperation>> All for E {
    type Output = E::Output;

    fn all(&self) -> Self::Output {
        self.build(AllOperation)
    }
}

operation_manifest! {
    AllOperation {
        method: All::all;
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
