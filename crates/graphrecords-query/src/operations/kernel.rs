use crate::{
    Arity, Bare, ElementShape, EvaluateOperand, IndexDomain, Indexed, Operand, QueryResult,
    ValueType,
    operands::OperandHandle,
    operations::{Apply, Dropping, Operation, Preserving, Retention},
    optimizer::{Estimate, Stats},
};
use graphrecords_core::GraphRecord;

pub type KeyedStream<'a, I, V, C> =
    <OperandHandle<Indexed<I, V>, C> as EvaluateOperand>::ReturnValue<'a>;

pub type BareStream<'a, V, C> = <OperandHandle<Bare<V>, C> as EvaluateOperand>::ReturnValue<'a>;

pub struct Pipeline<'a, X: 'a, Y: 'a, R: Retention> {
    run: Box<dyn FnMut(X) -> R::Step<Y> + 'a>,
}

impl<'a, T: 'a> Default for Pipeline<'a, T, T, Preserving> {
    fn default() -> Self {
        Self {
            run: Box::new(|element| element),
        }
    }
}

impl<'a, X: 'a, Y: 'a, R: Retention> Pipeline<'a, X, Y, R> {
    #[must_use]
    pub fn element_wise(function: impl FnMut(X) -> R::Step<Y> + 'a) -> Self {
        Self {
            run: Box::new(function),
        }
    }

    #[must_use]
    pub fn map<Z: 'a>(self, mut function: impl FnMut(Y) -> Z + 'a) -> Pipeline<'a, X, Z, R> {
        let mut run = self.run;

        Pipeline {
            run: Box::new(move |element| R::map_step(run(element), &mut function)),
        }
    }

    #[must_use]
    pub fn filter(
        self,
        mut predicate: impl FnMut(&Y) -> bool + 'a,
    ) -> Pipeline<'a, X, Y, Dropping> {
        let mut run = self.run;

        Pipeline {
            run: Box::new(move |element| {
                R::collapse(run(element)).filter(|value| predicate(value))
            }),
        }
    }

    #[must_use]
    pub fn filter_map<Z: 'a>(
        self,
        mut function: impl FnMut(Y) -> Option<Z> + 'a,
    ) -> Pipeline<'a, X, Z, Dropping> {
        let mut run = self.run;

        Pipeline {
            run: Box::new(move |element| R::collapse(run(element)).and_then(&mut function)),
        }
    }

    #[must_use]
    pub fn into_function(self) -> Box<dyn FnMut(X) -> R::Step<Y> + 'a> {
        self.run
    }
}

pub trait Kernel<S: ElementShape, C: Arity>: Operation {
    type Output: Operand;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: <OperandHandle<S, C> as EvaluateOperand>::ReturnValue<'a>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>;

    #[allow(unused_variables)]
    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        Estimate::UNKNOWN
    }
}

impl<K, V, C, P> Apply<P> for OperandHandle<Indexed<K, V>, C>
where
    K: IndexDomain,
    V: ValueType,
    C: Arity,
    P: Kernel<Indexed<K, V>, C>,
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

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        <P as Kernel<Indexed<K, V>, C>>::estimate(operation, input, stats)
    }
}

impl<V, C, P> Apply<P> for OperandHandle<Bare<V>, C>
where
    V: ValueType,
    C: Arity,
    P: Kernel<Bare<V>, C>,
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

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        <P as Kernel<Bare<V>, C>>::estimate(operation, input, stats)
    }
}

pub type ElementPipeline<'a, S, P> = Pipeline<
    'a,
    <S as ElementShape>::Element<'a>,
    <<P as ElementKernel<S>>::OutShape as ElementShape>::Element<'a>,
    <P as ElementKernel<S>>::Retention,
>;

pub trait ElementKernel<S: ElementShape>: Operation {
    type OutShape: ElementShape;
    type Retention: Retention;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, S, Self>>;

    #[allow(unused_variables)]
    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        input
    }
}

impl<S: ElementShape, C: Arity, P: ElementKernel<S>> Kernel<S, C> for P {
    type Output = OperandHandle<P::OutShape, <P::Retention as Retention>::OutArity<C>>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: <OperandHandle<S, C> as EvaluateOperand>::ReturnValue<'a>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let pipeline = P::pipeline(graphrecord, prepared)?;

        Ok(<P::Retention as Retention>::apply::<C, _, _>(
            values,
            pipeline.into_function(),
        ))
    }

    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        ElementKernel::estimate(self, input, stats)
    }
}
