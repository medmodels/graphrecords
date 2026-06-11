use crate::{
    Arity, Bare, ElementShape, EvaluateOperand, IndexDomain, Indexed, Operand, QueryResult,
    ValueType,
    operands::OperandHandle,
    operations::{Apply, Operation, Prepare},
    optimizer::EstimateCost,
};
use graphrecords_core::GraphRecord;

pub type KeyedStream<'a, I, V, C> =
    <OperandHandle<Indexed<I, V>, C> as EvaluateOperand>::ReturnValue<'a>;

pub type BareStream<'a, V, C> = <OperandHandle<Bare<V>, C> as EvaluateOperand>::ReturnValue<'a>;

pub trait Kernel<S: ElementShape, C: Arity>: Operation {
    type Output: Operand;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: <OperandHandle<S, C> as EvaluateOperand>::ReturnValue<'a>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>;
}

impl<K, V, C, P> Apply<P> for OperandHandle<Indexed<K, V>, C>
where
    K: IndexDomain,
    V: ValueType,
    C: Arity,
    P: Kernel<Indexed<K, V>, C>,
    Self: EstimateCost<P, OutputCost = <P::Output as Operand>::Cost>,
{
    type Output = P::Output;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <P as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        P::execute(graphrecord, values, prepared)
    }
}

impl<V, C, P> Apply<P> for OperandHandle<Bare<V>, C>
where
    V: ValueType,
    C: Arity,
    P: Kernel<Bare<V>, C>,
    Self: EstimateCost<P, OutputCost = <P::Output as Operand>::Cost>,
{
    type Output = P::Output;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: <P as Prepare>::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        P::execute(graphrecord, values, prepared)
    }
}
