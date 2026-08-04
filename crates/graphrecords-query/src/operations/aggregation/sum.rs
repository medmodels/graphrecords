use crate::{
    Bare, BareValueDomain, EvaluateOperand, Explain, IndexDomain, Indexed, Labeled, Multiple,
    Operand, OrderState, QueryResult, Single,
    capabilities::ValueAdd,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Sum,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Sum")]
#[plan(optimizer_hints(empty = if_any))]
pub struct SumOperation;

impl Prepare for SumOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V, O> LaneKernel<Indexed<I, V>, Multiple<O>> for SumOperation
where
    I: IndexDomain,
    V: ValueAdd + BareValueDomain,
    O: OrderState,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum, (index, value)| {
            let value = value?;

            match sum {
                Some(sum) => V::add(Self::LABEL, sum, value)
                    .map(Some)
                    .map_err(|failure| failure.at::<I>(&index)),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for SumOperation
where
    V: ValueAdd + BareValueDomain,
    O: OrderState,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let sum = values.try_fold(None, |sum, value| {
            let value = value?;

            match sum {
                Some(sum) => V::add(Self::LABEL, sum, value).map(Some),
                None => Ok(Some(value)),
            }
        });

        Ok(sum.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<SumOperation>> Sum for O {
    type ReturnOperand = O::Output;

    fn sum(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SumOperation))
    }
}

operation_manifest! {
    SumOperation {
        method: Sum::sum;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueAdd + BareValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: OperandHandle<Bare<V>, Single>;
        }

        kernel {
            parameters: <
                V: ValueAdd + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: OperandHandle<Bare<V>, Single>;
        }
    }
}
