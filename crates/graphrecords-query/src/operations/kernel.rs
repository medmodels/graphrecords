use crate::{
    Arity, Bare, BoxedIterator, ElementShape, EvaluateOperand, IndexDomain, Indexed, Multiple,
    Operand, OrderState, QueryResult, ValueType,
    operands::OperandHandle,
    operations::{Apply, Operation},
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
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        P::execute(graphrecord, values, prepared)
    }
}

pub struct Pipeline<'a, X: 'a, Y: 'a> {
    run: Box<dyn FnOnce(BoxedIterator<'a, X>) -> BoxedIterator<'a, Y> + 'a>,
}

impl<'a, T: 'a> Default for Pipeline<'a, T, T> {
    fn default() -> Self {
        Self {
            run: Box::new(|elements| elements),
        }
    }
}

impl<'a, X: 'a, Y: 'a> Pipeline<'a, X, Y> {
    #[must_use]
    pub fn map<Z: 'a>(self, function: impl FnMut(Y) -> Z + 'a) -> Pipeline<'a, X, Z> {
        Pipeline {
            run: Box::new(move |elements| Box::new((self.run)(elements).map(function))),
        }
    }

    #[must_use]
    pub fn filter(self, predicate: impl FnMut(&Y) -> bool + 'a) -> Self {
        Self {
            run: Box::new(move |elements| Box::new((self.run)(elements).filter(predicate))),
        }
    }

    #[must_use]
    pub fn filter_map<Z: 'a>(
        self,
        function: impl FnMut(Y) -> Option<Z> + 'a,
    ) -> Pipeline<'a, X, Z> {
        Pipeline {
            run: Box::new(move |elements| Box::new((self.run)(elements).filter_map(function))),
        }
    }

    #[must_use]
    pub fn scan<T: 'a, Z: 'a>(
        self,
        initial: T,
        mut function: impl FnMut(&mut T, Y) -> Option<Z> + 'a,
    ) -> Pipeline<'a, X, Z> {
        Pipeline {
            run: Box::new(move |elements| {
                Box::new(
                    (self.run)(elements)
                        .scan(initial, move |state, element| {
                            Some(function(state, element))
                        })
                        .flatten(),
                )
            }),
        }
    }

    #[must_use]
    pub fn execute(self, elements: BoxedIterator<'a, X>) -> BoxedIterator<'a, Y> {
        (self.run)(elements)
    }
}

pub trait ElementKernel<S: ElementShape>: Operation {
    type OutShape: ElementShape;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<Pipeline<'a, S::Element<'a>, <Self::OutShape as ElementShape>::Element<'a>>>;
}

impl<S: ElementShape, O: OrderState, P: ElementKernel<S>> Kernel<S, Multiple<O>> for P {
    type Output = OperandHandle<P::OutShape, Multiple<O>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: <OperandHandle<S, Multiple<O>> as EvaluateOperand>::ReturnValue<'a>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(P::pipeline(graphrecord, prepared)?.execute(values))
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
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        P::execute(graphrecord, values, prepared)
    }
}
