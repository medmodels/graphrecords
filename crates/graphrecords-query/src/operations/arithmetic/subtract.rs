use crate::{
    AttributeName, Bare, Explain, Failure, IndexDomain, IndexValue, Indexed, Labeled, Mask,
    Operand, Positional, QueryResult, Scalar, ValueType,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, BarePipeline, ElementKernel, ElementPipeline, IndexedValuePipeline,
        Keyed, Operation, OperationContext, Pipeline, Prepare, Retention, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Subtract,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex},
};
use std::ops::Sub;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Subtract")]
pub struct SubtractOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for SubtractOperation<A> {
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

fn subtract_indexed<'a, I, A, V>(
    prepared: A::Prepared<'a>,
    subtract: fn(V::Value<'a>, V::Value<'a>) -> GraphRecordResult<V::Value<'a>>,
) -> IndexedValuePipeline<'a, I, V, V, A::Retention>
where
    I: IndexDomain,
    A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
    A::Prepared<'a>: 'a,
    V: ValueType,
{
    let label = SubtractOperation::<A>::LABEL;

    Pipeline::keyed(move |index, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &index, label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                subtract(value, argument)
                    .map_err(|error| Failure::new_at::<I, _>(label, error, &index))
            })
        })
    })
}

fn subtract_bare<'a, A, V>(
    prepared: A::Prepared<'a>,
    subtract: fn(V::Value<'a>, V::Value<'a>) -> GraphRecordResult<V::Value<'a>>,
) -> BarePipeline<'a, V, V, A::Retention>
where
    A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
    A::Prepared<'a>: 'a,
    V: ValueType,
{
    let label = SubtractOperation::<A>::LABEL;

    Pipeline::new(move |item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &(), label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                subtract(value, argument).map_err(|error| Failure::new(label, error))
            })
        })
    })
}

impl<I, A> ElementKernel<Indexed<I, Scalar>> for SubtractOperation<A>
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
        Ok(subtract_indexed::<_, A, Scalar>(prepared, Sub::sub))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<Scalar>> for SubtractOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordValue>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Scalar>, Self>> {
        Ok(subtract_bare::<A, Scalar>(prepared, Sub::sub))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<I, A> ElementKernel<Indexed<I, Mask>> for SubtractOperation<A>
where
    I: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = bool>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, Mask>, Self>> {
        Ok(subtract_indexed::<_, A, Mask>(
            prepared,
            |value, argument| Ok(value && !argument),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<Mask>> for SubtractOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = bool>,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<Mask>, Self>> {
        Ok(subtract_bare::<A, Mask>(prepared, |value, argument| {
            Ok(value && !argument)
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input
        }
    }
}

impl<I, A> ElementKernel<Indexed<I, AttributeName>> for SubtractOperation<A>
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
        Ok(subtract_indexed::<_, A, AttributeName>(prepared, Sub::sub))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<AttributeName>> for SubtractOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordAttribute>,
{
    type Emission = A::Retention;
    type OutShape = Bare<AttributeName>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<AttributeName>, Self>> {
        Ok(subtract_bare::<A, AttributeName>(prepared, Sub::sub))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<K, A> ElementKernel<Indexed<K, IndexValue<Positional>>> for SubtractOperation<A>
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
        Ok(subtract_indexed::<_, A, IndexValue<Positional>>(
            prepared,
            |value, argument| Ok(value - argument),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<IndexValue<Positional>>> for SubtractOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = usize>,
{
    type Emission = A::Retention;
    type OutShape = Bare<IndexValue<Positional>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<Positional>>, Self>> {
        Ok(subtract_bare::<A, IndexValue<Positional>>(
            prepared,
            |value, argument| Ok(value - argument),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<K, A> ElementKernel<Indexed<K, IndexValue<NodeIndex>>> for SubtractOperation<A>
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
        Ok(subtract_indexed::<_, A, IndexValue<NodeIndex>>(
            prepared,
            Sub::sub,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<IndexValue<NodeIndex>>> for SubtractOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordAttribute>,
{
    type Emission = A::Retention;
    type OutShape = Bare<IndexValue<NodeIndex>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<NodeIndex>>, Self>> {
        Ok(subtract_bare::<A, IndexValue<NodeIndex>>(
            prepared,
            Sub::sub,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<K, A> ElementKernel<Indexed<K, IndexValue<EdgeIndex>>> for SubtractOperation<A>
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
        Ok(subtract_indexed::<_, A, IndexValue<EdgeIndex>>(
            prepared,
            |value, argument| Ok(value - argument),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<IndexValue<EdgeIndex>>> for SubtractOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = EdgeIndex>,
{
    type Emission = A::Retention;
    type OutShape = Bare<IndexValue<EdgeIndex>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<EdgeIndex>>, Self>> {
        Ok(subtract_bare::<A, IndexValue<EdgeIndex>>(
            prepared,
            |value, argument| Ok(value - argument),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<K, A> ElementKernel<Indexed<K, IndexValue<GraphRecordValue>>> for SubtractOperation<A>
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
        Ok(subtract_indexed::<_, A, IndexValue<GraphRecordValue>>(
            prepared,
            Sub::sub,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<IndexValue<GraphRecordValue>>> for SubtractOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = GraphRecordValue>,
{
    type Emission = A::Retention;
    type OutShape = Bare<IndexValue<GraphRecordValue>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<GraphRecordValue>>, Self>> {
        Ok(subtract_bare::<A, IndexValue<GraphRecordValue>>(
            prepared,
            Sub::sub,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<K, A> ElementKernel<Indexed<K, IndexValue<bool>>> for SubtractOperation<A>
where
    K: IndexDomain,
    for<'a> A: ArgumentSource<Keyed<K>, Value<'a> = bool>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<K, IndexValue<bool>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<K, IndexValue<bool>>, Self>> {
        Ok(subtract_indexed::<_, A, IndexValue<bool>>(
            prepared,
            |value, argument| Ok(value && !argument),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<A> ElementKernel<Bare<IndexValue<bool>>> for SubtractOperation<A>
where
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = bool>,
{
    type Emission = A::Retention;
    type OutShape = Bare<IndexValue<bool>>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<IndexValue<bool>>, Self>> {
        Ok(subtract_bare::<A, IndexValue<bool>>(
            prepared,
            |value, argument| Ok(value && !argument),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<O, A> Subtract<A> for O
where
    SubtractOperation<A>: Operation,
    O: Apply<SubtractOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn subtract(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            SubtractOperation { argument },
        ))
    }
}
