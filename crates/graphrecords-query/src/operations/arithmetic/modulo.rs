use crate::{
    AttributeName, Bare, Diagnostic, Explain, Failure, IndexDomain, IndexValue, Indexed, Labeled,
    Operand, Positional, QueryResult, Scalar, ValueType,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, BarePipeline, ElementKernel, ElementPipeline, IndexedValuePipeline,
        Keyed, Operation, OperationContext, Pipeline, Prepare, Retention, Unaligned,
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
#[operation(scope = Element)]
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
    is_modulo_by_zero: fn(&V::Value<'a>, &V::Value<'a>) -> bool,
    modulo: fn(V::Value<'a>, V::Value<'a>) -> GraphRecordResult<V::Value<'a>>,
) -> IndexedValuePipeline<'a, I, V, V, A::Retention>
where
    I: IndexDomain,
    A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
    A::Prepared<'a>: 'a,
    V: ValueType,
{
    let label = ModuloOperation::<A>::LABEL;

    Pipeline::keyed(move |index, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &index, label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|modulus| {
                if is_modulo_by_zero(&value, &modulus) {
                    return Err(Failure::new_at::<I, _>(label, ModuloByZero, &index));
                }

                modulo(value, modulus)
                    .map_err(|error| Failure::new_at::<I, _>(label, error, &index))
            })
        })
    })
}

fn modulo_bare<'a, A, V>(
    prepared: A::Prepared<'a>,
    is_modulo_by_zero: fn(&V::Value<'a>, &V::Value<'a>) -> bool,
    modulo: fn(V::Value<'a>, V::Value<'a>) -> GraphRecordResult<V::Value<'a>>,
) -> BarePipeline<'a, V, V, A::Retention>
where
    A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
    A::Prepared<'a>: 'a,
    V: ValueType,
{
    let label = ModuloOperation::<A>::LABEL;

    Pipeline::new(move |item| {
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
    type Emission = A::Retention;
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Scalar>, Self>> {
        Ok(modulo_indexed::<_, A, Scalar>(
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
    type Emission = A::Retention;
    type OutShape = Bare<Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Scalar>, Self>> {
        Ok(modulo_bare::<A, Scalar>(
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
    type Emission = A::Retention;
    type OutShape = Indexed<I, AttributeName>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, AttributeName>, Self>> {
        Ok(modulo_indexed::<_, A, AttributeName>(
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
    type Emission = A::Retention;
    type OutShape = Bare<AttributeName>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<AttributeName>, Self>> {
        Ok(modulo_bare::<A, AttributeName>(
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
    type Emission = A::Retention;
    type OutShape = Indexed<K, IndexValue<Positional>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<Positional>>, Self>> {
        Ok(modulo_indexed::<_, A, IndexValue<Positional>>(
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
    type Emission = A::Retention;
    type OutShape = Bare<IndexValue<Positional>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<Positional>>, Self>> {
        Ok(modulo_bare::<A, IndexValue<Positional>>(
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
    type Emission = A::Retention;
    type OutShape = Indexed<K, IndexValue<NodeIndex>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<NodeIndex>>, Self>> {
        Ok(modulo_indexed::<_, A, IndexValue<NodeIndex>>(
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
    type Emission = A::Retention;
    type OutShape = Bare<IndexValue<NodeIndex>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<NodeIndex>>, Self>> {
        Ok(modulo_bare::<A, IndexValue<NodeIndex>>(
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
    type Emission = A::Retention;
    type OutShape = Indexed<K, IndexValue<EdgeIndex>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<EdgeIndex>>, Self>> {
        Ok(modulo_indexed::<_, A, IndexValue<EdgeIndex>>(
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
    type Emission = A::Retention;
    type OutShape = Bare<IndexValue<EdgeIndex>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<EdgeIndex>>, Self>> {
        Ok(modulo_bare::<A, IndexValue<EdgeIndex>>(
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
    type Emission = A::Retention;
    type OutShape = Indexed<K, IndexValue<GraphRecordValue>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<GraphRecordValue>>, Self>> {
        Ok(modulo_indexed::<_, A, IndexValue<GraphRecordValue>>(
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
    type Emission = A::Retention;
    type OutShape = Bare<IndexValue<GraphRecordValue>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<GraphRecordValue>>, Self>> {
        Ok(modulo_bare::<A, IndexValue<GraphRecordValue>>(
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
    type ReturnOperand = O::Output;

    fn modulo(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            ModuloOperation { argument },
        ))
    }
}
