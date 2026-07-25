use crate::{
    AttributeName, Bare, Diagnostic, Explain, Failure, IndexDomain, IndexValue, Indexed, Labeled,
    Operand, Positional, QueryResult, Scalar, ToOwnedValue,
    execution::EvaluationCache,
    operations::{
        Alignment, Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation,
        OperationContext, Pipeline, Prepare, Retention, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Modulo,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex, datatypes::Mod},
};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

type IndexedModuloElement<'a, I, V> = (<Keyed<I> as Alignment>::Address<'a>, QueryResult<V>);

#[derive(Debug)]
pub struct ModuloByZero;

impl Display for ModuloByZero {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("cannot calculate a remainder with a zero modulus")
    }
}

impl Error for ModuloByZero {}

impl Diagnostic for ModuloByZero {
    fn name() -> &'static str {
        "ModuloByZero"
    }
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[explain(label = "Modulo")]
pub struct ModuloOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for ModuloOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.argument.prepare(graphrecord, cache)
    }
}

fn is_graphrecord_value_modulo_by_zero(
    value: &GraphRecordValue,
    modulus: &GraphRecordValue,
) -> bool {
    match (value, modulus) {
        (GraphRecordValue::Int(_) | GraphRecordValue::Float(_), GraphRecordValue::Int(0)) => true,
        (
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_),
            GraphRecordValue::Float(modulus),
        ) => *modulus == 0.0,
        _ => false,
    }
}

const fn is_attribute_modulo_by_zero(
    value: &GraphRecordAttribute,
    modulus: &GraphRecordAttribute,
) -> bool {
    matches!(
        (value, modulus),
        (GraphRecordAttribute::Int(_), GraphRecordAttribute::Int(0))
    )
}

fn modulo_indexed<'a, I, A, V>(
    prepared: A::Prepared<'a>,
    is_modulo_by_zero: fn(&V, &V) -> bool,
    modulo: fn(V, V) -> GraphRecordResult<V>,
) -> Pipeline<'a, IndexedModuloElement<'a, I, V>, IndexedModuloElement<'a, I, V>, A::Retention>
where
    I: IndexDomain,
    A: ArgumentSource<Keyed<I>, Value<'a> = V>,
    A::Prepared<'a>: 'a,
    V: Clone + ToOwnedValue + 'a,
{
    let label = ModuloOperation::<A>::LABEL;

    Pipeline::element_wise(move |(index, item)| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep((index, Err(original)));
            }
        };

        let step = A::resolve(&prepared, &index, label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            let result = resolved.and_then(|modulus| {
                if is_modulo_by_zero(&value, &modulus) {
                    return Err(Failure::new_at(label, ModuloByZero, &index));
                }

                modulo(value, modulus).map_err(|error| Failure::new_at(label, error, &index))
            });

            (index, result)
        })
    })
}

fn modulo_bare<'a, A, V>(
    prepared: A::Prepared<'a>,
    is_modulo_by_zero: fn(&V, &V) -> bool,
    modulo: fn(V, V) -> GraphRecordResult<V>,
) -> Pipeline<'a, QueryResult<V>, QueryResult<V>, A::Retention>
where
    A: ArgumentSource<Unaligned, Value<'a> = V>,
    A::Prepared<'a>: 'a,
    V: Clone + ToOwnedValue + 'a,
{
    let label = ModuloOperation::<A>::LABEL;

    Pipeline::element_wise(move |item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &(), label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|modulus| {
                if is_modulo_by_zero(&value, &modulus) {
                    return Err(Failure::new(label, ModuloByZero));
                }

                modulo(value, modulus).map_err(|error| Failure::new(label, error))
            })
        })
    })
}

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
        Ok(modulo_indexed::<I, A, GraphRecordValue>(
            prepared,
            is_graphrecord_value_modulo_by_zero,
            Mod::r#mod,
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
        Ok(modulo_bare::<A, GraphRecordValue>(
            prepared,
            is_graphrecord_value_modulo_by_zero,
            Mod::r#mod,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<I, A> ElementKernel<Indexed<I, AttributeName>> for ModuloOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = GraphRecordAttribute>,
{
    type OutShape = Indexed<I, AttributeName>;
    type Retention = <A as ArgumentSource<Keyed<I>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, AttributeName>, Self>> {
        Ok(modulo_indexed::<I, A, GraphRecordAttribute>(
            prepared,
            is_attribute_modulo_by_zero,
            Mod::r#mod,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<AttributeName>> for ModuloOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordAttribute>,
{
    type OutShape = Bare<AttributeName>;
    type Retention = <A as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<AttributeName>, Self>> {
        Ok(modulo_bare::<A, GraphRecordAttribute>(
            prepared,
            is_attribute_modulo_by_zero,
            Mod::r#mod,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<K, A> ElementKernel<Indexed<K, IndexValue<Positional>>> for ModuloOperation<A>
where
    K: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<K>, Value<'a> = usize>,
{
    type OutShape = Indexed<K, IndexValue<Positional>>;
    type Retention = <A as ArgumentSource<Keyed<K>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<Positional>>, Self>> {
        Ok(modulo_indexed::<K, A, usize>(
            prepared,
            |_, modulus| *modulus == 0,
            |value, modulus| Ok(value % modulus),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<IndexValue<Positional>>> for ModuloOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = usize>,
{
    type OutShape = Bare<IndexValue<Positional>>;
    type Retention = <A as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<Positional>>, Self>> {
        Ok(modulo_bare::<A, usize>(
            prepared,
            |_, modulus| *modulus == 0,
            |value, modulus| Ok(value % modulus),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<K, A> ElementKernel<Indexed<K, IndexValue<NodeIndex>>> for ModuloOperation<A>
where
    K: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<K>, Value<'a> = GraphRecordAttribute>,
{
    type OutShape = Indexed<K, IndexValue<NodeIndex>>;
    type Retention = <A as ArgumentSource<Keyed<K>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<NodeIndex>>, Self>> {
        Ok(modulo_indexed::<K, A, GraphRecordAttribute>(
            prepared,
            is_attribute_modulo_by_zero,
            Mod::r#mod,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<IndexValue<NodeIndex>>> for ModuloOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordAttribute>,
{
    type OutShape = Bare<IndexValue<NodeIndex>>;
    type Retention = <A as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<NodeIndex>>, Self>> {
        Ok(modulo_bare::<A, GraphRecordAttribute>(
            prepared,
            is_attribute_modulo_by_zero,
            Mod::r#mod,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<K, A> ElementKernel<Indexed<K, IndexValue<EdgeIndex>>> for ModuloOperation<A>
where
    K: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<K>, Value<'a> = EdgeIndex>,
{
    type OutShape = Indexed<K, IndexValue<EdgeIndex>>;
    type Retention = <A as ArgumentSource<Keyed<K>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<EdgeIndex>>, Self>> {
        Ok(modulo_indexed::<K, A, EdgeIndex>(
            prepared,
            |_, modulus| *modulus == 0,
            Mod::r#mod,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<IndexValue<EdgeIndex>>> for ModuloOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = EdgeIndex>,
{
    type OutShape = Bare<IndexValue<EdgeIndex>>;
    type Retention = <A as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<EdgeIndex>>, Self>> {
        Ok(modulo_bare::<A, EdgeIndex>(
            prepared,
            |_, modulus| *modulus == 0,
            Mod::r#mod,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<K, A> ElementKernel<Indexed<K, IndexValue<GraphRecordValue>>> for ModuloOperation<A>
where
    K: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<K>, Value<'a> = GraphRecordValue>,
{
    type OutShape = Indexed<K, IndexValue<GraphRecordValue>>;
    type Retention = <A as ArgumentSource<Keyed<K>>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<GraphRecordValue>>, Self>> {
        Ok(modulo_indexed::<K, A, GraphRecordValue>(
            prepared,
            is_graphrecord_value_modulo_by_zero,
            Mod::r#mod,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<IndexValue<GraphRecordValue>>> for ModuloOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordValue>,
{
    type OutShape = Bare<IndexValue<GraphRecordValue>>;
    type Retention = <A as ArgumentSource<Unaligned>>::Retention;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<GraphRecordValue>>, Self>> {
        Ok(modulo_bare::<A, GraphRecordValue>(
            prepared,
            is_graphrecord_value_modulo_by_zero,
            Mod::r#mod,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<O, A> Modulo<A> for O
where
    ModuloOperation<A>: Operation,
    O: Apply<ModuloOperation<A>>,
{
    type ReturnOperand = <O as Apply<ModuloOperation<A>>>::Output;

    fn modulo(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            ModuloOperation { argument },
        ))
    }
}
