use crate::{
    ElementShape, EvaluateOperand, Multiple, Operand, QueryResult,
    operands::OperandHandle,
    operations::{Apply, ElementKernel, Kernel},
    optimizer::{EstimateCost, Stats},
};
use graphrecords_core::GraphRecord;
use std::marker::PhantomData;

pub struct Ordered<S>(PhantomData<S>);

impl<S: ElementShape> ElementShape for Ordered<S> {
    type Cost = S::Cost;
    type Element<'a> = S::Element<'a>;
}

impl<S, P> Apply<P> for OperandHandle<Ordered<S>, Multiple>
where
    S: ElementShape,
    P: ElementKernel<Ordered<S>>,
    OperandHandle<S, Multiple>: EstimateCost<
            P,
            OutputCost = <<P as ElementKernel<Ordered<S>>>::OutShape as ElementShape>::Cost,
        >,
    Self: Operand<Cost = <OperandHandle<S, Multiple> as Operand>::Cost>,
{
    type Output = OperandHandle<<P as ElementKernel<Ordered<S>>>::OutShape, Multiple>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        <P as Kernel<Ordered<S>, Multiple>>::execute(graphrecord, values, prepared)
    }
}

impl<S, P> EstimateCost<P> for OperandHandle<Ordered<S>, Multiple>
where
    S: ElementShape,
    P: ElementKernel<Ordered<S>>,
    OperandHandle<S, Multiple>: EstimateCost<
            P,
            OutputCost = <<P as ElementKernel<Ordered<S>>>::OutShape as ElementShape>::Cost,
        >,
    Self: Operand<Cost = <OperandHandle<S, Multiple> as Operand>::Cost>,
{
    type OutputCost = <<P as ElementKernel<Ordered<S>>>::OutShape as ElementShape>::Cost;

    fn estimate(
        operation: &P,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost {
        <OperandHandle<S, Multiple> as EstimateCost<P>>::estimate(operation, input_cost, stats)
    }
}
