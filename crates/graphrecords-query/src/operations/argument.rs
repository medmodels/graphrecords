use crate::{
    Diagnostic, Explain, Failure, IndexDomain, QueryResult, ToOwnedValue,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::{
    error::Error,
    fmt::{self, Display, Formatter, Write},
    hash::Hasher,
    marker::PhantomData,
};

pub enum Looked<'a, W> {
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
            Self::Empty => formatter.write_str("argument operand was empty"),
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
    fn help(&self) -> Option<String> {
        Some(
            "make the argument cover the subject's elements or state a policy with `on_missing(...)`"
                .to_string(),
        )
    }
}

#[derive(Clone, Copy)]
pub enum OnMissing {
    Raise,
    Drop,
}

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
        Failure::new_at(operation, cause, address)
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

pub trait ArgumentSource<A: Alignment>:
    Prepare + Explain + PlanIdentity + PlanInputs + Estimated
{
    type Value<'a>: Clone + ToOwnedValue
    where
        Self: 'a;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        address: &A::Address<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a;

    fn resolve<'a>(
        prepared: &Self::Prepared<'a>,
        address: &A::Address<'a>,
        label: &'static str,
        default: OnMissing,
    ) -> QueryResult<Option<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match Self::lookup(prepared, address) {
            Looked::Present(wrapped) => wrapped.clone().map(Some),
            Looked::Absent(absent) => match default {
                OnMissing::Drop => Ok(None),
                OnMissing::Raise => Err(A::raise_at(
                    label,
                    ArgumentAbsent { cause: absent },
                    address,
                )),
            },
        }
    }
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
        std::hash::Hash::hash(self, state);
    }
}

impl PlanInputs for GraphRecordValue {}

impl PlanIdentity for bool {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self, state);
    }
}

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
        Estimate {
            distinct: Some(1),
            ..Estimate::UNKNOWN
        }
    }
}

impl<A: Alignment> ArgumentSource<A> for GraphRecordValue {
    type Value<'a> = Self;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Looked::Present(prepared)
    }
}
