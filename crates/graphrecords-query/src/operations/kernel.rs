use crate::{
    Arity, Bare, ElementShape, EvaluateExpression, Expression, IndexDomain, Indexed, QueryResult,
    element::{ElementEmission, ElementTransition},
    expressions::{ExpressionHandle, GroupedExpression, Partition},
    operations::{Apply, Element, Group, Lane, Operation, OperationScope},
    optimizer::{Estimate, Stats},
};
use graphrecords_core::GraphRecord;

pub type KeyedStream<'a, I, V, C> =
    <ExpressionHandle<Indexed<I, V>, C> as EvaluateExpression>::ReturnValue<'a>;

pub type BareStream<'a, V, C> =
    <ExpressionHandle<Bare<V>, C> as EvaluateExpression>::ReturnValue<'a>;

pub type ElementPipeline<'a, S, P> = <S as ElementTransition<
    <P as ElementKernel<S>>::OutShape,
    <P as ElementKernel<S>>::Emission,
>>::Pipeline<'a>;

pub trait ElementKernel<S: ElementShape + ElementTransition<Self::OutShape, Self::Emission>>:
    Operation<Scope = Element>
{
    type OutShape: ElementShape;
    type Emission: ElementEmission;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, S, Self>>;

    #[allow(unused_variables)]
    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        Self::Emission::default_estimate(input)
    }
}

pub trait LaneKernel<S: ElementShape, C: Arity>: Operation<Scope = Lane> {
    type Output: Expression;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: <ExpressionHandle<S, C> as EvaluateExpression>::ReturnValue<'a>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>>;

    #[allow(unused_variables)]
    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        Estimate::UNKNOWN
    }
}

pub trait GroupKernel<M: IndexDomain, K: IndexDomain, E: Expression>:
    Operation<Scope = Group>
{
    type Output: Expression;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, E>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>>;

    #[allow(unused_variables)]
    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        Estimate::UNKNOWN
    }
}

impl<S, C, P> Apply<P, Element> for ExpressionHandle<S, C>
where
    S: ElementShape + ElementTransition<P::OutShape, P::Emission>,
    C: Arity,
    P: ElementKernel<S>,
{
    type Output = ExpressionHandle<P::OutShape, <P::Emission as ElementEmission>::OutArity<C>>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let pipeline = P::pipeline(graphrecord, prepared)?;

        Ok(S::apply(graphrecord, values, pipeline))
    }

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        operation.estimate(input, stats)
    }
}

impl<S: ElementShape, C: Arity, P: LaneKernel<S, C>> Apply<P, Lane> for ExpressionHandle<S, C> {
    type Output = P::Output;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        P::execute(graphrecord, values, prepared)
    }

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        operation.estimate(input, stats)
    }
}

impl<M, K, S, C, P> Apply<P, Element> for GroupedExpression<M, K, ExpressionHandle<S, C>>
where
    M: IndexDomain,
    K: IndexDomain,
    S: ElementShape + ElementTransition<P::OutShape, P::Emission>,
    C: Arity,
    P: ElementKernel<S>,
{
    type Output = GroupedExpression<M, K, <ExpressionHandle<S, C> as Apply<P, Element>>::Output>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(values.map_payloads(|_, _, payload| {
            payload.and_then(|values| {
                <ExpressionHandle<S, C> as Apply<P, Element>>::apply(
                    graphrecord,
                    values,
                    prepared.clone(),
                )
            })
        }))
    }

    fn estimate(operation: &P, mut input: Estimate, stats: &Stats) -> Estimate {
        input.per_group = input.per_group.map(|estimate| {
            Box::new(<ExpressionHandle<S, C> as Apply<P, Element>>::estimate(
                operation, *estimate, stats,
            ))
        });
        input
    }
}

impl<M: IndexDomain, K: IndexDomain, S: ElementShape, C: Arity, P: LaneKernel<S, C>> Apply<P, Lane>
    for GroupedExpression<M, K, ExpressionHandle<S, C>>
{
    type Output = GroupedExpression<M, K, <ExpressionHandle<S, C> as Apply<P, Lane>>::Output>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(values.map_payloads(|_, _, payload| {
            payload.and_then(|values| {
                <ExpressionHandle<S, C> as Apply<P, Lane>>::apply(
                    graphrecord,
                    values,
                    prepared.clone(),
                )
            })
        }))
    }

    fn estimate(operation: &P, mut input: Estimate, stats: &Stats) -> Estimate {
        input.per_group = input.per_group.map(|estimate| {
            Box::new(<ExpressionHandle<S, C> as Apply<P, Lane>>::estimate(
                operation, *estimate, stats,
            ))
        });
        input
    }
}

impl<M, K, S, C, P> Apply<P, Group> for GroupedExpression<M, K, ExpressionHandle<S, C>>
where
    M: IndexDomain,
    K: IndexDomain,
    S: ElementShape,
    C: Arity,
    P: GroupKernel<M, K, ExpressionHandle<S, C>>,
{
    type Output = P::Output;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        P::execute(graphrecord, values, prepared)
    }

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        operation.estimate(input, stats)
    }
}

impl<M, K, N, L, E, P, S> Apply<P, S> for GroupedExpression<M, K, GroupedExpression<N, L, E>>
where
    M: IndexDomain,
    K: IndexDomain,
    N: IndexDomain,
    L: IndexDomain,
    E: Expression,
    P: Operation<Scope = S>,
    S: OperationScope,
    GroupedExpression<N, L, E>: Apply<P, S>,
{
    type Output = GroupedExpression<M, K, <GroupedExpression<N, L, E> as Apply<P, S>>::Output>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(values.map_payloads(|_, _, payload| {
            payload.and_then(|values| {
                <GroupedExpression<N, L, E> as Apply<P, S>>::apply(
                    graphrecord,
                    values,
                    prepared.clone(),
                )
            })
        }))
    }

    fn estimate(operation: &P, mut input: Estimate, stats: &Stats) -> Estimate {
        input.per_group = input.per_group.map(|estimate| {
            Box::new(<GroupedExpression<N, L, E> as Apply<P, S>>::estimate(
                operation, *estimate, stats,
            ))
        });
        input
    }
}
