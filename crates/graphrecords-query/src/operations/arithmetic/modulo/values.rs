use super::ModuloOperation;
use crate::{
    Bare, Failure, IndexDomain, Indexed, Labeled, QueryResult, Scalar,
    operations::{
        ArgumentSource, ElementKernel, ElementPipeline, Keyed, Pipeline, Retention, Unaligned,
    },
    optimizer::{Estimate, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordValue, datatypes::Mod},
};

impl<I, A> ElementKernel<Indexed<I, Scalar>> for ModuloOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordValue>,
{
    type OutShape = Indexed<I, Scalar>;
    type Retention = <A as ArgumentSource<Keyed<I>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Scalar>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |(index, item): (I::Index<'a>, QueryResult<GraphRecordValue>)| {
                let value = match item {
                    Ok(value) => value,
                    Err(original) => {
                        return <Self::Retention as Retention>::keep((index, Err(original)));
                    }
                };

                let step = A::resolve(&prepared, &index, label);

                <Self::Retention as Retention>::map_step(step, |resolved| {
                    let result = resolved.and_then(|modulus| {
                        value
                            .r#mod(modulus)
                            .map_err(|error| Failure::new_at(label, error, &index))
                    });

                    (index, result)
                })
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<Scalar>> for ModuloOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordValue>,
{
    type OutShape = Bare<Scalar>;
    type Retention = <A as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Scalar>, Self>> {
        let label = Self::LABEL;

        Ok(Pipeline::element_wise(
            move |item: QueryResult<GraphRecordValue>| {
                let value = match item {
                    Ok(value) => value,
                    Err(original) => {
                        return <Self::Retention as Retention>::keep(Err(original));
                    }
                };

                let step = A::resolve(&prepared, &(), label);

                <Self::Retention as Retention>::map_step(step, |resolved| {
                    resolved.and_then(|modulus| {
                        value
                            .r#mod(modulus)
                            .map_err(|error| Failure::new(label, error))
                    })
                })
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}
