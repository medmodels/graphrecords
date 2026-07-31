use crate::{
    Bare, BareValueType, EvaluateOperand, Explain, IndexDomain, Indexed, Labeled, Multiple,
    Operand, OrderState, QueryResult, Single,
    capabilities::ValueMultiply,
    execution::EvaluationCache,
    operands::OperandHandle,
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Product,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Product")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ProductOperation;

impl Prepare for ProductOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V, O> LaneKernel<Indexed<I, V>, Multiple<O>> for ProductOperation
where
    I: IndexDomain,
    V: ValueMultiply + BareValueType,
    O: OrderState,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let product = values.try_fold(None, |product, (index, value)| {
            let value = value?;

            match product {
                Some(product) => V::multiply(Self::LABEL, product, value)
                    .map(Some)
                    .map_err(|failure| failure.at::<I>(&index)),
                None => Ok(Some(value)),
            }
        });

        Ok(product.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V, O> LaneKernel<Bare<V>, Multiple<O>> for ProductOperation
where
    V: ValueMultiply + BareValueType,
    O: OrderState,
{
    type Output = OperandHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let product = values.try_fold(None, |product, value| {
            let value = value?;

            match product {
                Some(product) => V::multiply(Self::LABEL, product, value).map(Some),
                None => Ok(Some(value)),
            }
        });

        Ok(product.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<O: Apply<ProductOperation>> Product for O {
    type ReturnOperand = O::Output;

    fn product(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), ProductOperation))
    }
}
