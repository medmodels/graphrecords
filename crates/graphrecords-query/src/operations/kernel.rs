use crate::{
    Arity, Bare, ElementShape, EvaluateOperand, IndexDomain, Indexed, Operand, QueryResult,
    element::{ElementEmission, ElementTransition},
    index::GroupKey,
    operands::{GroupOperand, OperandHandle, Partition},
    operations::{Apply, Element, Group, Lane, Operation, OperationScope},
    optimizer::{Estimate, Stats},
};
use graphrecords_core::GraphRecord;

pub type KeyedStream<'a, I, V, C> =
    <OperandHandle<Indexed<I, V>, C> as EvaluateOperand>::ReturnValue<'a>;

pub type BareStream<'a, V, C> = <OperandHandle<Bare<V>, C> as EvaluateOperand>::ReturnValue<'a>;

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

pub trait GroupKernel<M: IndexDomain, K: GroupKey, O: Operand>: Operation<Scope = Group> {
    type Output: Operand;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        partition: Partition<'a, M, K, O>,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>;

    #[allow(unused_variables)]
    fn estimate(&self, input: Estimate, stats: &Stats) -> Estimate {
        Estimate::UNKNOWN
    }
}

impl<S, C, P> Apply<P, Element> for OperandHandle<S, C>
where
    S: ElementShape + ElementTransition<P::OutShape, P::Emission>,
    C: Arity,
    P: ElementKernel<S>,
{
    type Output = OperandHandle<P::OutShape, <P::Emission as ElementEmission>::OutArity<C>>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        let pipeline = P::pipeline(graphrecord, prepared)?;

        Ok(S::apply(values, pipeline))
    }

    fn estimate(operation: &P, input: Estimate, stats: &Stats) -> Estimate {
        operation.estimate(input, stats)
    }
}

impl<S: ElementShape, C: Arity, P: LaneKernel<S, C>> Apply<P, Lane> for OperandHandle<S, C> {
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
        operation.estimate(input, stats)
    }
}

impl<M, K, S, C, P> Apply<P, Element> for GroupOperand<M, K, OperandHandle<S, C>>
where
    M: IndexDomain,
    K: GroupKey,
    S: ElementShape + ElementTransition<P::OutShape, P::Emission>,
    C: Arity,
    P: ElementKernel<S>,
{
    type Output = GroupOperand<M, K, <OperandHandle<S, C> as Apply<P, Element>>::Output>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(values.map_payloads(|_, _, payload| {
            payload.and_then(|values| {
                <OperandHandle<S, C> as Apply<P, Element>>::apply(
                    graphrecord,
                    values,
                    prepared.clone(),
                )
            })
        }))
    }

    fn estimate(operation: &P, mut input: Estimate, stats: &Stats) -> Estimate {
        input.per_group = input.per_group.map(|estimate| {
            Box::new(<OperandHandle<S, C> as Apply<P, Element>>::estimate(
                operation, *estimate, stats,
            ))
        });
        input
    }
}

impl<M: IndexDomain, K: GroupKey, S: ElementShape, C: Arity, P: LaneKernel<S, C>> Apply<P, Lane>
    for GroupOperand<M, K, OperandHandle<S, C>>
{
    type Output = GroupOperand<M, K, <OperandHandle<S, C> as Apply<P, Lane>>::Output>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(values.map_payloads(|_, _, payload| {
            payload.and_then(|values| {
                <OperandHandle<S, C> as Apply<P, Lane>>::apply(
                    graphrecord,
                    values,
                    prepared.clone(),
                )
            })
        }))
    }

    fn estimate(operation: &P, mut input: Estimate, stats: &Stats) -> Estimate {
        input.per_group = input.per_group.map(|estimate| {
            Box::new(<OperandHandle<S, C> as Apply<P, Lane>>::estimate(
                operation, *estimate, stats,
            ))
        });
        input
    }
}

impl<M, K, S, C, P> Apply<P, Group> for GroupOperand<M, K, OperandHandle<S, C>>
where
    M: IndexDomain,
    K: GroupKey,
    S: ElementShape,
    C: Arity,
    P: GroupKernel<M, K, OperandHandle<S, C>>,
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
        operation.estimate(input, stats)
    }
}

impl<M, K, N, L, O, P, S> Apply<P, S> for GroupOperand<M, K, GroupOperand<N, L, O>>
where
    M: IndexDomain,
    K: GroupKey,
    N: IndexDomain,
    L: GroupKey,
    O: Operand,
    P: Operation<Scope = S>,
    S: OperationScope,
    GroupOperand<N, L, O>: Apply<P, S>,
{
    type Output = GroupOperand<M, K, <GroupOperand<N, L, O> as Apply<P, S>>::Output>;

    fn apply<'a>(
        graphrecord: &'a GraphRecord,
        values: Self::ReturnValue<'a>,
        prepared: P::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>>
    where
        Self: 'a,
    {
        Ok(values.map_payloads(|_, _, payload| {
            payload.and_then(|values| {
                <GroupOperand<N, L, O> as Apply<P, S>>::apply(graphrecord, values, prepared.clone())
            })
        }))
    }

    fn estimate(operation: &P, mut input: Estimate, stats: &Stats) -> Estimate {
        input.per_group = input.per_group.map(|estimate| {
            Box::new(<GroupOperand<N, L, O> as Apply<P, S>>::estimate(
                operation, *estimate, stats,
            ))
        });
        input
    }
}
