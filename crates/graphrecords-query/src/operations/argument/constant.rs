use crate::{
    Explain, FailureKind, FailureKindValue, IndexValue, Mask, Position, QueryResult, Scalar,
    ValueDomain,
    element::Preserving,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    index::Positional,
    operations::{Alignment, ArgumentSource, Lookup, Prepare, SourceDomain},
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeName, EdgeIndex, Group, NodeIndex, Value},
};
use std::{
    fmt::{self, Write},
    hash::{Hash, Hasher},
};

impl Explain for Value {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for Value {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for Value {}

impl Prepare for Value {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for Value {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for Value {
    type ValueDomain = Scalar;
}

impl<A, V> ArgumentSource<A, V> for Value
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
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
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
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
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

impl Explain for AttributeName {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for AttributeName {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for AttributeName {}

impl Prepare for AttributeName {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for AttributeName {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for AttributeName {
    type ValueDomain = Self;
}

impl<A, V> ArgumentSource<A, V> for AttributeName
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
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
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
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
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
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
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
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
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
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
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
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
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

impl Explain for NodeIndex {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for NodeIndex {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for NodeIndex {}

impl Prepare for NodeIndex {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for NodeIndex {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for NodeIndex {
    type ValueDomain = IndexValue<Self>;
}

impl<A, V> ArgumentSource<A, V> for NodeIndex
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

impl Explain for Group {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for Group {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for Group {}

impl Prepare for Group {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for Group {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for Group {
    type ValueDomain = IndexValue<Self>;
}

impl<A, V> ArgumentSource<A, V> for Group
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}
