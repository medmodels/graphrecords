use super::{
    DynArityStream, DynCachedValue, DynIndex, DynIndexAddress, DynIndexOwned, DynStream, DynValue,
};
use crate::{
    EvaluateExpression, Expression, ExpressionContext, Failure, IndexDomain, QueryResult,
    ValueDomain,
    execution::{CacheableExpression, EvaluationCache},
    expressions::{GroupedExpression, OwnedPartition, Partition},
    optimizer::{Estimate, Estimated, PlanNode, Stats},
};
use graphrecords_core::GraphRecord;
use std::sync::Arc;

pub struct DynPayload {
    context: Arc<dyn ExpressionContext<Self>>,
}

pub enum DynYield<'a> {
    Lane(DynStream<'a>),
    Group(Partition<'a, DynIndex, DynIndex, DynPayload>),
}

pub enum DynArityContainer<T> {
    MultipleOrdered(Vec<T>),
    MultipleUnordered(Vec<T>),
    Single(Option<T>),
    Definite(T),
}

pub enum DynCachedLane {
    IndexedValue(DynArityContainer<(DynIndexAddress, QueryResult<DynCachedValue>)>),
    IndexedMask(DynArityContainer<(DynIndexAddress, QueryResult<bool>)>),
    IndexedUnit(DynArityContainer<(DynIndexAddress, QueryResult<()>)>),
    BareValue(DynArityContainer<QueryResult<DynCachedValue>>),
    BareMask(DynArityContainer<QueryResult<bool>>),
}

pub enum DynCached {
    Lane(DynCachedLane),
    Group(OwnedPartition<DynIndex, DynIndex, Self>),
}

pub enum DynTerminalLane {
    IndexedValue(DynArityContainer<(DynIndexOwned, QueryResult<DynValue>)>),
    IndexedMask(DynArityContainer<(DynIndexOwned, QueryResult<bool>)>),
    IndexedUnit(DynArityContainer<(DynIndexOwned, QueryResult<()>)>),
    BareValue(DynArityContainer<QueryResult<DynValue>>),
    BareMask(DynArityContainer<QueryResult<bool>>),
}

pub enum DynTerminal {
    Lane(DynTerminalLane),
    Group(DynTerminalPartition),
}

pub struct DynTerminalPartition {
    buckets: Vec<DynTerminalBucket>,
    key_failures: Vec<DynTerminalKeyFailure>,
}

pub struct DynTerminalBucket {
    key: DynIndexOwned,
    members: Vec<DynIndexOwned>,
    payload: QueryResult<DynTerminal>,
}

pub struct DynTerminalKeyFailure {
    member: DynIndexOwned,
    failure: Box<Failure>,
}

pub type DynTerminalPartitionParts = (Vec<DynTerminalBucket>, Vec<DynTerminalKeyFailure>);

impl<T> DynArityContainer<T> {
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

impl DynCachedLane {
    fn from_stream(stream: DynStream<'_>) -> Self {
        match stream {
            DynStream::IndexedValue(stream) => Self::IndexedValue(DynArityContainer::from_stream(
                stream,
                |(address, outcome)| (address, outcome.map(DynValue::into_cached)),
            )),
            DynStream::IndexedMask(stream) => {
                Self::IndexedMask(DynArityContainer::from_stream(stream, |element| element))
            }
            DynStream::IndexedUnit(stream) => {
                Self::IndexedUnit(DynArityContainer::from_stream(stream, |element| element))
            }
            DynStream::BareValue(stream) => {
                Self::BareValue(DynArityContainer::from_stream(stream, |outcome| {
                    outcome.map(DynValue::into_cached)
                }))
            }
            DynStream::BareMask(stream) => {
                Self::BareMask(DynArityContainer::from_stream(stream, |outcome| outcome))
            }
        }
    }

    fn to_stream<'a>(&'a self, graphrecord: &'a GraphRecord) -> DynStream<'a> {
        match self {
            Self::IndexedValue(stream) => {
                DynStream::IndexedValue(stream.to_stream(|(address, outcome)| {
                    (
                        address.clone(),
                        outcome
                            .as_ref()
                            .map(|cached| DynValue::from_cached(graphrecord, cached))
                            .map_err(Clone::clone),
                    )
                }))
            }
            Self::IndexedMask(stream) => DynStream::IndexedMask(
                stream.to_stream(|(address, outcome)| (address.clone(), outcome.clone())),
            ),
            Self::IndexedUnit(stream) => DynStream::IndexedUnit(
                stream.to_stream(|(address, outcome)| (address.clone(), outcome.clone())),
            ),
            Self::BareValue(stream) => DynStream::BareValue(stream.to_stream(|outcome| {
                outcome
                    .as_ref()
                    .map(|cached| DynValue::from_cached(graphrecord, cached))
                    .map_err(Clone::clone)
            })),
            Self::BareMask(stream) => DynStream::BareMask(stream.to_stream(QueryResult::clone)),
        }
    }
}

impl DynCached {
    fn from_yield(yielded: DynYield<'_>) -> Self {
        match yielded {
            DynYield::Lane(stream) => Self::Lane(DynCachedLane::from_stream(stream)),
            DynYield::Group(partition) => Self::Group(GroupedExpression::into_cached(partition)),
        }
    }

    fn to_yield<'a>(&'a self, graphrecord: &'a GraphRecord) -> DynYield<'a> {
        match self {
            Self::Lane(stream) => DynYield::Lane(stream.to_stream(graphrecord)),
            Self::Group(partition) => {
                DynYield::Group(GroupedExpression::from_cached(graphrecord, partition))
            }
        }
    }
}

impl DynTerminalLane {
    fn from_stream(graphrecord: &GraphRecord, stream: DynStream<'_>) -> Self {
        match stream {
            DynStream::IndexedValue(stream) => Self::IndexedValue(DynArityContainer::from_stream(
                stream,
                |(address, outcome)| {
                    (
                        DynIndex::own_index(&DynIndex::index(graphrecord, &address)),
                        outcome.map(DynValue::into_owned),
                    )
                },
            )),
            DynStream::IndexedMask(stream) => Self::IndexedMask(DynArityContainer::from_stream(
                stream,
                |(address, outcome)| {
                    (
                        DynIndex::own_index(&DynIndex::index(graphrecord, &address)),
                        outcome,
                    )
                },
            )),
            DynStream::IndexedUnit(stream) => Self::IndexedUnit(DynArityContainer::from_stream(
                stream,
                |(address, outcome)| {
                    (
                        DynIndex::own_index(&DynIndex::index(graphrecord, &address)),
                        outcome,
                    )
                },
            )),
            DynStream::BareValue(stream) => {
                Self::BareValue(DynArityContainer::from_stream(stream, |outcome| {
                    outcome.map(DynValue::into_owned)
                }))
            }
            DynStream::BareMask(stream) => {
                Self::BareMask(DynArityContainer::from_stream(stream, |outcome| outcome))
            }
        }
    }
}

impl DynTerminal {
    pub(crate) fn from_yield(graphrecord: &GraphRecord, yielded: DynYield<'_>) -> Self {
        match yielded {
            DynYield::Lane(stream) => Self::Lane(DynTerminalLane::from_stream(graphrecord, stream)),
            DynYield::Group(partition) => {
                Self::Group(DynTerminalPartition::from_partition(graphrecord, partition))
            }
        }
    }
}

impl DynTerminalPartition {
    fn from_partition(
        graphrecord: &GraphRecord,
        partition: Partition<'_, DynIndex, DynIndex, DynPayload>,
    ) -> Self {
        let (buckets, key_failures) = partition.into_parts();

        Self {
            buckets: buckets
                .into_iter()
                .map(|(key, members, payload)| DynTerminalBucket {
                    key,
                    members: members
                        .iter()
                        .map(|member| DynIndex::own_index(&DynIndex::index(graphrecord, member)))
                        .collect(),
                    payload: payload.map(|payload| DynTerminal::from_yield(graphrecord, payload)),
                })
                .collect(),
            key_failures: key_failures
                .into_iter()
                .map(|(member, failure)| DynTerminalKeyFailure {
                    member: DynIndex::own_index(&DynIndex::index(graphrecord, &member)),
                    failure,
                })
                .collect(),
        }
    }

    #[must_use]
    pub fn buckets(&self) -> &[DynTerminalBucket] {
        &self.buckets
    }

    #[must_use]
    pub fn key_failures(&self) -> &[DynTerminalKeyFailure] {
        &self.key_failures
    }

    #[must_use]
    pub fn into_parts(self) -> DynTerminalPartitionParts {
        (self.buckets, self.key_failures)
    }
}

impl DynTerminalBucket {
    #[must_use]
    pub const fn key(&self) -> &DynIndexOwned {
        &self.key
    }

    #[must_use]
    pub fn members(&self) -> &[DynIndexOwned] {
        &self.members
    }

    pub const fn payload(&self) -> &QueryResult<DynTerminal> {
        &self.payload
    }

    pub fn into_parts(self) -> (DynIndexOwned, Vec<DynIndexOwned>, QueryResult<DynTerminal>) {
        (self.key, self.members, self.payload)
    }
}

impl DynTerminalKeyFailure {
    #[must_use]
    pub const fn member(&self) -> &DynIndexOwned {
        &self.member
    }

    #[must_use]
    pub fn failure(&self) -> &Failure {
        &self.failure
    }

    #[must_use]
    pub fn into_parts(self) -> (DynIndexOwned, Box<Failure>) {
        (self.member, self.failure)
    }
}

impl Clone for DynPayload {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl EvaluateExpression for DynPayload {
    type ReturnValue<'a>
        = DynYield<'a>
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, cache)
    }
}

impl Estimated for DynPayload {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.context.estimate(stats)
    }
}

impl Expression for DynPayload {
    fn context(&self) -> &dyn ExpressionContext<Self> {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<dyn ExpressionContext<Self>>) -> Self {
        Self { context }
    }
}

impl CacheableExpression for DynPayload {
    type Cached = DynCached;

    fn into_cached(values: Self::ReturnValue<'_>) -> Self::Cached {
        DynCached::from_yield(values)
    }

    fn from_cached<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::Cached,
    ) -> Self::ReturnValue<'a> {
        cached.to_yield(graphrecord)
    }
}
