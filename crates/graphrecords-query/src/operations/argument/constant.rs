use crate::{
    AttributeName, Explain, FailureKind, FailureKindValue, IndexValue, Mask, Position, QueryResult,
    Scalar, ValueDomain,
    element::Preserving,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    index::Positional,
    operations::{Alignment, ArgumentSource, Lookup, Prepare, SourceDomain},
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue},
};
use std::{
    fmt::{self, Write},
    hash::{Hash, Hasher},
};

impl Explain for GraphRecordValue {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for GraphRecordValue {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for GraphRecordValue {}

impl Prepare for GraphRecordValue {
    type Prepared<'a> = QueryResult<Self>;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Ok(self.clone()))
    }
}

impl Estimated for GraphRecordValue {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for GraphRecordValue {
    type ValueDomain = Scalar;
}

impl<A, V> ArgumentSource<A, V> for GraphRecordValue
where
    A: Alignment,
    for<'a> V: ValueDomain<Value<'a> = Self>,
{
    type Retention = Preserving;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}

impl Explain for bool {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for bool {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for bool {}

impl Prepare for bool {
    type Prepared<'a> = QueryResult<Self>;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Ok(*self))
    }
}

impl Estimated for bool {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: Some(if *self { 1.0 } else { 0.0 }),
            ..Estimate::singleton()
        }
    }
}

impl SourceDomain for bool {
    type ValueDomain = Mask;
}

impl<A, V> ArgumentSource<A, V> for bool
where
    A: Alignment,
    for<'a> V: ValueDomain<Value<'a> = Self>,
{
    type Retention = Preserving;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}

impl Explain for GraphRecordAttribute {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for GraphRecordAttribute {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for GraphRecordAttribute {}

impl Prepare for GraphRecordAttribute {
    type Prepared<'a> = QueryResult<Self>;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Ok(self.clone()))
    }
}

impl Estimated for GraphRecordAttribute {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for GraphRecordAttribute {
    type ValueDomain = AttributeName;
}

impl<A, V> ArgumentSource<A, V> for GraphRecordAttribute
where
    A: Alignment,
    for<'a> V: ValueDomain<Value<'a> = Self>,
{
    type Retention = Preserving;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}

impl Explain for EdgeIndex {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for EdgeIndex {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for EdgeIndex {}

impl Prepare for EdgeIndex {
    type Prepared<'a> = QueryResult<Self>;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Ok(*self))
    }
}

impl Estimated for EdgeIndex {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for EdgeIndex {
    type ValueDomain = IndexValue<Self>;
}

impl<A, V> ArgumentSource<A, V> for EdgeIndex
where
    A: Alignment,
    for<'a> V: ValueDomain<Value<'a> = Self>,
{
    type Retention = Preserving;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}

impl Explain for Position {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for Position {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for Position {}

impl Prepare for Position {
    type Prepared<'a> = QueryResult<Self>;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Ok(*self))
    }
}

impl Estimated for Position {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for Position {
    type ValueDomain = IndexValue<Positional>;
}

impl<A, V> ArgumentSource<A, V> for Position
where
    A: Alignment,
    for<'a> V: ValueDomain<Value<'a> = Self>,
{
    type Retention = Preserving;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}

impl Explain for FailureKind {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for FailureKind {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for FailureKind {}

impl Prepare for FailureKind {
    type Prepared<'a> = QueryResult<Self>;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Ok(*self))
    }
}

impl Estimated for FailureKind {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for FailureKind {
    type ValueDomain = FailureKindValue;
}

impl<A, V> ArgumentSource<A, V> for FailureKind
where
    A: Alignment,
    for<'a> V: ValueDomain<Value<'a> = Self>,
{
    type Retention = Preserving;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}
