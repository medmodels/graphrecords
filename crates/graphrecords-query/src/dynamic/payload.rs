use super::{DynArityStream, DynIndex, DynIndexOwned, DynStream, DynValue};
use crate::{
    EvaluateOperand, IndexDomain, Operand, OperandContext, QueryResult,
    execution::{CacheableOperand, EvaluationCache},
    operands::{GroupOperand, Partition, PartitionOwned},
    optimizer::{Estimate, Estimated, PlanNode, Stats},
};
use graphrecords_core::GraphRecord;
use std::sync::Arc;

pub struct DynPayload {
    context: Arc<dyn OperandContext<Self>>,
}

pub enum DynYield<'a> {
    Lane(DynStream<'a>),
    Group(Partition<'a, DynIndex, DynIndex, DynPayload>),
}

pub enum DynTerminalArity<T> {
    MultipleOrdered(Vec<T>),
    MultipleUnordered(Vec<T>),
    Single(Option<T>),
    Definite(T),
}

pub enum DynTerminalLane {
    IndexedValue(DynTerminalArity<(DynIndexOwned, QueryResult<DynValue>)>),
    IndexedMask(DynTerminalArity<(DynIndexOwned, QueryResult<bool>)>),
    IndexedUnit(DynTerminalArity<(DynIndexOwned, QueryResult<()>)>),
    BareValue(DynTerminalArity<QueryResult<DynValue>>),
    BareMask(DynTerminalArity<QueryResult<bool>>),
}

pub enum DynTerminal {
    Lane(DynTerminalLane),
    Group(PartitionOwned<DynIndex, DynIndex, Self>),
}

impl<T> DynTerminalArity<T> {
    fn from_stream<'a, U: 'a>(stream: DynArityStream<'a, U>, function: impl Fn(U) -> T) -> Self {
        match stream {
            DynArityStream::MultipleOrdered(stream) => {
                Self::MultipleOrdered(stream.map(function).collect())
            }
            DynArityStream::MultipleUnordered(stream) => {
                Self::MultipleUnordered(stream.map(function).collect())
            }
            DynArityStream::Single(element) => Self::Single(element.map(function)),
            DynArityStream::Definite(element) => Self::Definite(function(element)),
        }
    }

    fn to_stream<'a, U: 'a>(&'a self, function: impl Fn(&'a T) -> U + 'a) -> DynArityStream<'a, U> {
        match self {
            Self::MultipleOrdered(elements) => {
                DynArityStream::MultipleOrdered(Box::new(elements.iter().map(function)))
            }
            Self::MultipleUnordered(elements) => {
                DynArityStream::MultipleUnordered(Box::new(elements.iter().map(function)))
            }
            Self::Single(element) => DynArityStream::Single(element.as_ref().map(function)),
            Self::Definite(element) => DynArityStream::Definite(function(element)),
        }
    }
}

impl DynTerminalLane {
    pub(crate) fn from_stream(stream: DynStream<'_>) -> Self {
        match stream {
            DynStream::IndexedValue(stream) => {
                Self::IndexedValue(DynTerminalArity::from_stream(stream, |(index, outcome)| {
                    (<DynIndex as IndexDomain>::to_owned(&index), outcome)
                }))
            }
            DynStream::IndexedMask(stream) => {
                Self::IndexedMask(DynTerminalArity::from_stream(stream, |(index, outcome)| {
                    (<DynIndex as IndexDomain>::to_owned(&index), outcome)
                }))
            }
            DynStream::IndexedUnit(stream) => {
                Self::IndexedUnit(DynTerminalArity::from_stream(stream, |(index, outcome)| {
                    (<DynIndex as IndexDomain>::to_owned(&index), outcome)
                }))
            }
            DynStream::BareValue(stream) => {
                Self::BareValue(DynTerminalArity::from_stream(stream, |outcome| outcome))
            }
            DynStream::BareMask(stream) => {
                Self::BareMask(DynTerminalArity::from_stream(stream, |outcome| outcome))
            }
        }
    }

    fn to_stream(&self) -> DynStream<'_> {
        match self {
            Self::IndexedValue(stream) => DynStream::IndexedValue(
                stream.to_stream(|(index, outcome)| (DynIndex::from_owned(index), outcome.clone())),
            ),
            Self::IndexedMask(stream) => DynStream::IndexedMask(
                stream.to_stream(|(index, outcome)| (DynIndex::from_owned(index), outcome.clone())),
            ),
            Self::IndexedUnit(stream) => DynStream::IndexedUnit(
                stream.to_stream(|(index, outcome)| (DynIndex::from_owned(index), outcome.clone())),
            ),
            Self::BareValue(stream) => DynStream::BareValue(stream.to_stream(QueryResult::clone)),
            Self::BareMask(stream) => DynStream::BareMask(stream.to_stream(QueryResult::clone)),
        }
    }
}

impl DynTerminal {
    pub(crate) fn from_yield(yielded: DynYield<'_>) -> Self {
        match yielded {
            DynYield::Lane(stream) => Self::Lane(DynTerminalLane::from_stream(stream)),
            DynYield::Group(partition) => Self::Group(
                <GroupOperand<DynIndex, DynIndex, DynPayload> as CacheableOperand>::into_cached(
                    partition,
                ),
            ),
        }
    }

    fn to_yield(&self) -> DynYield<'_> {
        match self {
            Self::Lane(stream) => DynYield::Lane(stream.to_stream()),
            Self::Group(partition) => DynYield::Group(
                <GroupOperand<DynIndex, DynIndex, DynPayload> as CacheableOperand>::from_cached(
                    partition,
                ),
            ),
        }
    }
}

impl Clone for DynPayload {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl EvaluateOperand for DynPayload {
    type ReturnValue<'a>
        = DynYield<'a>
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, cache)
    }
}

impl Estimated for DynPayload {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.context.estimate(stats)
    }
}

impl Operand for DynPayload {
    fn context(&self) -> &dyn OperandContext<Self> {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<dyn OperandContext<Self>>) -> Self {
        Self { context }
    }
}

impl CacheableOperand for DynPayload {
    type Cached = DynTerminal;

    fn into_cached(values: Self::ReturnValue<'_>) -> Self::Cached {
        DynTerminal::from_yield(values)
    }

    fn from_cached(cached: &Self::Cached) -> Self::ReturnValue<'_> {
        cached.to_yield()
    }
}
