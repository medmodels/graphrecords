use crate::{
    Arity, Diagnostic, ElementShape, Explain, Failure, IndexDomain, Position, QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{ElementEmission, Preserving, Retention},
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue},
};
use std::{
    error::Error,
    fmt::{self, Display, Formatter, Write},
    hash::{Hash, Hasher},
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
