use crate::{
    Bare, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult, ValueType,
    element::{Pipeline, Retention},
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Clip,
    value::ValueClip,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Clip")]
#[plan(optimizer_hints(empty = if_all))]
pub struct ClipOperation<L, U> {
    #[argument]
    lower: L,
    #[argument]
    upper: U,
}

impl<L: Prepare, U: Prepare> Prepare for ClipOperation<L, U> {
    type Prepared<'a>
        = (L::Prepared<'a>, U::Prepared<'a>)
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((
            self.lower.prepare(graphrecord, cache)?,
            self.upper.prepare(graphrecord, cache)?,
        ))
    }
}

impl<I, V, L, U> ElementKernel<Indexed<I, V>> for ClipOperation<L, U>
where
    I: IndexDomain,
    for<'a> V: ValueClip + ValueType<Value<'a> = <V as ValueType>::Owned>,
    for<'a> L: ArgumentSource<Keyed<I>, Value<'a> = <V as ValueType>::Owned>,
    for<'a> U: ArgumentSource<Keyed<I>, Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = <L::Retention as Retention>::Then<U::Retention>;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::keyed(move |index, item| {
            let value = match item {
                Ok(value) => value,
                Err(failure) => {
                    return <<L::Retention as Retention>::Then<U::Retention> as Retention>::keep(
                        Err(failure),
                    );
                }
            };

            let lower = L::resolve(&prepared.0, &index, Self::LABEL);

            <L::Retention as Retention>::and_then(lower, |lower| {
                let upper = U::resolve(&prepared.1, &index, Self::LABEL);

                <U::Retention as Retention>::map_step(upper, |upper| {
                    upper.and_then(|upper| {
                        V::clip(Self::LABEL, value, lower, upper)
                            .map_err(|failure| failure.at::<I>(&index))
                    })
                })
            })
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, L, U> ElementKernel<Bare<V>> for ClipOperation<L, U>
where
    for<'a> V: ValueClip + ValueType<Value<'a> = <V as ValueType>::Owned>,
    for<'a> L: ArgumentSource<Unaligned, Value<'a> = <V as ValueType>::Owned>,
    for<'a> U: ArgumentSource<Unaligned, Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = <L::Retention as Retention>::Then<U::Retention>;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(move |item| {
            let value = match item {
                Ok(value) => value,
                Err(failure) => {
                    return <<L::Retention as Retention>::Then<U::Retention> as Retention>::keep(
                        Err(failure),
                    );
                }
            };

            let lower = L::resolve(&prepared.0, &(), Self::LABEL);

            <L::Retention as Retention>::and_then(lower, |lower| {
                let upper = U::resolve(&prepared.1, &(), Self::LABEL);

                <U::Retention as Retention>::map_step(upper, |upper| {
                    upper.and_then(|upper| V::clip(Self::LABEL, value, lower, upper))
                })
            })
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O, L, U> Clip<L, U> for O
where
    ClipOperation<L, U>: Operation,
    O: Apply<ClipOperation<L, U>>,
{
    type ReturnOperand = O::Output;

    fn clip(&self, lower: L, upper: U) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            ClipOperation { lower, upper },
        ))
    }
}
