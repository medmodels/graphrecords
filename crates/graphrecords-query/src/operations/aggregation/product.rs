use crate::{
    Bare, BareValueDomain, EvaluateExpression, Explain, IndexDomain, Indexed, Labeled, Multiple,
    OrderState, QueryResult, Single,
    capabilities::ValueMultiply,
    expressions::ExpressionHandle,
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Product,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Product")]
#[plan(optimizer_hints(empty = if_any))]
pub struct ProductOperation;

impl<I: IndexDomain, V: ValueMultiply + BareValueDomain, O: OrderState>
    LaneKernel<Indexed<I, V>, Multiple<O>> for ProductOperation
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        mut values: KeyedStream<'a, I, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let product = values.try_fold(None, |product, (address, value)| {
            let value = value?;

            match product {
                Some(product) => V::multiply(product, value, Self::LABEL)
                    .map(Some)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address)),
                None => Ok(Some(value)),
            }
        });

        Ok(product.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<V: ValueMultiply + BareValueDomain, O: OrderState> LaneKernel<Bare<V>, Multiple<O>>
    for ProductOperation
{
    type Output = ExpressionHandle<Bare<V>, Single>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        mut values: BareStream<'a, V, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let product = values.try_fold(None, |product, value| {
            let value = value?;

            match product {
                Some(product) => V::multiply(product, value, Self::LABEL).map(Some),
                None => Ok(Some(value)),
            }
        });

        Ok(product.transpose())
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.zero_or_one()
    }
}

impl<E: Build<ProductOperation>> Product for E {
    type Output = E::Output;

    fn product(&self) -> Self::Output {
        self.build(ProductOperation)
    }
}

operation_manifest! {
    ProductOperation {
        method: Product::product;
        scope: lane;

        kernel {
            parameters: <
                I: IndexDomain,
                V: ValueMultiply + BareValueDomain,
                O: OrderState,
            >;
            input: (Indexed<I, V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
        }

        kernel {
            parameters: <
                V: ValueMultiply + BareValueDomain,
                O: OrderState,
            >;
            input: (Bare<V>, Multiple<O>);
            output: ExpressionHandle<Bare<V>, Single>;
        }
    }
}
