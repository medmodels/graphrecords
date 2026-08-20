use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueClip,
    element::{Pipeline, Retention},
    execution::EvaluationCache,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::Clip,
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
        cache: &'a EvaluationCache,
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
    V: ValueClip,
    L: ArgumentSource<Keyed<I>, V>,
    U: ArgumentSource<Keyed<I>, V>,
{
    type Emission = <L::Retention as Retention>::Then<U::Retention>;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::keyed(move |address, item| {
            let value = match item {
                Ok(value) => value,
                Err(failure) => {
                    return Self::Emission::keep(Err(failure));
                }
            };

            let lower = L::resolve(graphrecord, &prepared.0, &address, Self::LABEL);

            L::Retention::and_then(lower, |lower| {
                let upper = U::resolve(graphrecord, &prepared.1, &address, Self::LABEL);

                U::Retention::map_step(upper, |upper| {
                    upper.and_then(|upper| {
                        V::clip(value, lower, upper, Self::LABEL)
                            .map_err(|failure| failure.at_address::<I>(graphrecord, &address))
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
    V: ValueClip + BareValueDomain,
    L: ArgumentSource<Unaligned, V>,
    U: ArgumentSource<Unaligned, V>,
{
    type Emission = <L::Retention as Retention>::Then<U::Retention>;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(move |item| {
            let value = match item {
                Ok(value) => value,
                Err(failure) => {
                    return Self::Emission::keep(Err(failure));
                }
            };

            let lower = L::resolve(graphrecord, &prepared.0, &(), Self::LABEL);

            L::Retention::and_then(lower, |lower| {
                let upper = U::resolve(graphrecord, &prepared.1, &(), Self::LABEL);

                U::Retention::map_step(upper, |upper| {
                    upper.and_then(|upper| V::clip(value, lower, upper, Self::LABEL))
                })
            })
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E, L, U> Clip<L, U> for E
where
    ClipOperation<L, U>: Operation,
    E: Build<ClipOperation<L, U>>,
{
    type Output = E::Output;

    fn clip(&self, lower: L, upper: U) -> Self::Output {
        self.build(ClipOperation { lower, upper })
    }
}

operation_manifest! {
    ClipOperation<L, U> {
        method: Clip<L, U>::clip;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueClip>;
            argument: L: ArgumentSource<Keyed<I>, V>;
            argument: U: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueClip + BareValueDomain>;
            argument: L: ArgumentSource<Unaligned, V>;
            argument: U: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
