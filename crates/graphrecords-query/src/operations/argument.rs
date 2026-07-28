use crate::{
    Arity, Diagnostic, ElementShape, Explain, Failure, FailureKind, IndexDomain, Position,
    QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{ElementEmission, Preserving, Retention},
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue},
};
use graphrecords_utils::aliases::GrHashSet;
use std::{
    collections::HashSet,
    error::Error,
    fmt::{self, Display, Formatter, Write},
    hash::{BuildHasher, DefaultHasher, Hash, Hasher},
    marker::PhantomData,
};

pub trait Prepare: 'static {
    type Prepared<'a>: Clone + 'a
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>>;
}

pub trait Alignment: 'static {
    type Address<'a>;

    fn raise_at(
        operation: &'static str,
        cause: impl Diagnostic,
        address: &Self::Address<'_>,
    ) -> Box<Failure>;
}

pub struct Keyed<I: IndexDomain>(PhantomData<I>);

impl<I: IndexDomain> Alignment for Keyed<I> {
    type Address<'a> = I::Index<'a>;

    fn raise_at(
        operation: &'static str,
        cause: impl Diagnostic,
        address: &Self::Address<'_>,
    ) -> Box<Failure> {
        Failure::new_at::<I, _>(operation, cause, address)
    }
}

pub struct Unaligned;

impl Alignment for Unaligned {
    type Address<'a> = ();

    fn raise_at(
        operation: &'static str,
        cause: impl Diagnostic,
        _address: &Self::Address<'_>,
    ) -> Box<Failure> {
        Failure::new(operation, cause)
    }
}

pub enum Lookup<'a, W> {
    Present(&'a W),
    Absent(Absent),
}

#[derive(Debug, Clone, Copy)]
pub enum Absent {
    Uncovered,
    Empty,
}

impl Display for Absent {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uncovered => formatter.write_str("argument did not cover this index"),
            Self::Empty => formatter.write_str("argument provided no value for this lookup"),
        }
    }
}

impl Error for Absent {}

#[derive(Debug)]
pub struct ArgumentAbsent {
    pub cause: Absent,
}

impl Display for ArgumentAbsent {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.cause)
    }
}

impl Error for ArgumentAbsent {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}

impl Diagnostic for ArgumentAbsent {
    fn name() -> &'static str {
        "ArgumentAbsent"
    }

    fn help(&self) -> Option<String> {
        Some(
            "make the argument cover the subject's elements or state a policy with `on_missing(...)`"
                .to_string(),
        )
    }
}

pub trait ArgumentSource<A: Alignment>:
    Prepare + Explain + PlanIdentity + PlanInputs + Estimated
{
    type Value<'a>: Clone
    where
        Self: 'a;

    type OwnedValue: 'static;

    type Retention: Retention;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a;

    fn resolve<'a>(
        prepared: &Self::Prepared<'a>,
        address: &A::Address<'a>,
        label: &'static str,
    ) -> <Self::Retention as ElementEmission>::Step<QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match Self::lookup(prepared, address) {
            Lookup::Present(wrapped) => <Self::Retention as Retention>::keep(wrapped.clone()),
            Lookup::Absent(absent) => <Self::Retention as Retention>::absent(|| {
                A::raise_at(label, ArgumentAbsent { cause: absent }, address)
            }),
        }
    }
}

pub type IndexedElementContainer<'a, I, V, C> =
    <C as Arity>::Container<'a, (<I as IndexDomain>::Index<'a>, QueryResult<V>)>;

pub trait IndexedElementSource<I: IndexDomain>: Prepare {
    type Value<'a>: Clone
    where
        Self: 'a;

    type Arity: Arity;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> IndexedElementContainer<'a, I, Self::Value<'a>, Self::Arity>
    where
        Self: 'a;
}

pub trait SetSource: Prepare + Explain + PlanIdentity + PlanInputs + Estimated {
    type Value<'a>: Eq + Hash
    where
        Self: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a;
}

pub trait PreparedArity<S: ElementShape>: Arity {
    type Prepared<'a>: Clone + 'a
    where
        S: 'a;

    fn prepare<'a>(
        container: Self::Container<'a, S::Element<'a>>,
    ) -> QueryResult<Self::Prepared<'a>>
    where
        S: 'a;
}

pub trait AlignableArity<S: ElementShape, A: Alignment>: PreparedArity<S> {
    type Value<'a>: Clone
    where
        S: 'a;

    type OwnedValue: 'static;

    type Retention: Retention;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        S: 'a;
}

pub trait EnumerableArity<S: ElementShape, I: IndexDomain>: PreparedArity<S> {
    type Value<'a>: Clone
    where
        S: 'a;

    fn elements<'a>(
        prepared: Self::Prepared<'a>,
    ) -> IndexedElementContainer<'a, I, Self::Value<'a>, Self>
    where
        S: 'a;
}

pub trait SetArity<S: ElementShape>: PreparedArity<S> {
    type Value<'a>: Eq + Hash
    where
        S: 'a;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        S: 'a;
}

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
        Hash::hash(self, state);
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

impl<A: Alignment> ArgumentSource<A> for GraphRecordValue {
    type OwnedValue = Self;
    type Retention = Preserving;
    type Value<'a> = Self;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        value.clone()
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
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
        Hash::hash(self, state);
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

impl<A: Alignment> ArgumentSource<A> for bool {
    type OwnedValue = Self;
    type Retention = Preserving;
    type Value<'a> = Self;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        *value
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
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
        Hash::hash(self, state);
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

impl<A: Alignment> ArgumentSource<A> for GraphRecordAttribute {
    type OwnedValue = Self;
    type Retention = Preserving;
    type Value<'a> = Self;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        value.clone()
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
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
        Hash::hash(self, state);
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

impl<A: Alignment> ArgumentSource<A> for EdgeIndex {
    type OwnedValue = Self;
    type Retention = Preserving;
    type Value<'a> = Self;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        *value
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
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
        Hash::hash(self, state);
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

impl<A: Alignment> ArgumentSource<A> for Position {
    type OwnedValue = Self;
    type Retention = Preserving;
    type Value<'a> = Self;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        *value
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
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
        Hash::hash(self, state);
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

impl<A: Alignment> ArgumentSource<A> for FailureKind {
    type OwnedValue = Self;
    type Retention = Preserving;
    type Value<'a> = Self;

    fn to_owned_value(value: &Self::Value<'_>) -> Self::OwnedValue {
        *value
    }

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(prepared)
    }
}

impl<T: Display> Explain for Vec<T> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.write_char('[')?;

        for (position, member) in self.iter().enumerate() {
            if position > 0 {
                formatter.write_str(", ")?;
            }

            write!(formatter, "{member}")?;
        }

        formatter.write_char(']')
    }
}

impl<T: PartialEq + Hash> PlanIdentity for Vec<T> {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(self, state);
    }
}

impl<T: Clone> PlanInputs for Vec<T> {}

impl<T: 'static> Prepare for Vec<T> {
    type Prepared<'a> = &'a [T];

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T> Estimated for Vec<T> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::values(self.len(), self.len())
    }
}

impl<T> SetSource for Vec<T>
where
    T: 'static + Clone + Eq + Hash + Display,
{
    type Value<'a> = T;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Ok(prepared.iter().cloned().collect())
    }
}

impl<T: Display, const N: usize> Explain for [T; N] {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.write_char('[')?;

        for (position, member) in self.iter().enumerate() {
            if position > 0 {
                formatter.write_str(", ")?;
            }

            write!(formatter, "{member}")?;
        }

        formatter.write_char(']')
    }
}

impl<T: PartialEq + Hash, const N: usize> PlanIdentity for [T; N] {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(self, state);
    }
}

impl<T: Clone, const N: usize> PlanInputs for [T; N] {}

impl<T: 'static, const N: usize> Prepare for [T; N] {
    type Prepared<'a> = &'a [T];

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T, const N: usize> Estimated for [T; N] {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::values(N, N)
    }
}

impl<T, const N: usize> SetSource for [T; N]
where
    T: 'static + Clone + Eq + Hash + Display,
{
    type Value<'a> = T;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Ok(prepared.iter().cloned().collect())
    }
}

impl<T: Display> Explain for GrHashSet<T> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        let mut members: Vec<_> = self.iter().map(ToString::to_string).collect();
        members.sort_unstable();

        formatter.write_char('[')?;

        for (position, member) in members.iter().enumerate() {
            if position > 0 {
                formatter.write_str(", ")?;
            }

            formatter.write_str(member)?;
        }

        formatter.write_char(']')
    }
}

impl<T: Eq + Hash> PlanIdentity for GrHashSet<T> {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.len());

        let combined = self
            .iter()
            .map(|member| {
                let mut hasher = DefaultHasher::new();
                member.hash(&mut hasher);
                hasher.finish()
            })
            .fold(0_u64, u64::wrapping_add);

        state.write_u64(combined);
    }
}

impl<T: Clone> PlanInputs for GrHashSet<T> {}

impl<T: 'static> Prepare for GrHashSet<T> {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T> Estimated for GrHashSet<T> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::values(self.len(), self.len())
    }
}

impl<T> SetSource for GrHashSet<T>
where
    T: 'static + Clone + Eq + Hash + Display,
{
    type Value<'a> = T;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Ok(prepared.clone())
    }
}

impl<T: Display, S> Explain for HashSet<T, S> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        let mut members: Vec<_> = self.iter().map(ToString::to_string).collect();
        members.sort_unstable();

        formatter.write_char('[')?;

        for (position, member) in members.iter().enumerate() {
            if position > 0 {
                formatter.write_str(", ")?;
            }

            formatter.write_str(member)?;
        }

        formatter.write_char(']')
    }
}

impl<T: Eq + Hash, S: BuildHasher> PlanIdentity for HashSet<T, S> {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.len());

        let combined = self
            .iter()
            .map(|member| {
                let mut hasher = DefaultHasher::new();
                member.hash(&mut hasher);
                hasher.finish()
            })
            .fold(0_u64, u64::wrapping_add);

        state.write_u64(combined);
    }
}

impl<T: Clone, S: Clone> PlanInputs for HashSet<T, S> {}

impl<T: 'static, S: 'static> Prepare for HashSet<T, S> {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T, S> Estimated for HashSet<T, S> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::values(self.len(), self.len())
    }
}

impl<T, S> SetSource for HashSet<T, S>
where
    T: 'static + Clone + Eq + Hash + Display,
    S: 'static + Clone + BuildHasher,
{
    type Value<'a> = T;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Ok(prepared.iter().cloned().collect())
    }
}
