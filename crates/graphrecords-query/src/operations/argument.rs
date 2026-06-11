use crate::{
    Explain, Failure, IndexDomain, QueryResult,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    optimizer::{PlanIdentity, PlanInputs},
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use std::{
    error::Error,
    fmt::{self, Display, Formatter, Write},
    hash::Hasher,
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

pub trait ArgumentSource<I: IndexDomain>: Prepare + Explain + PlanIdentity + PlanInputs {
    type Value: 'static + Clone;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        index: &I::Index<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value>>
    where
        Self: 'a;

    fn resolve<'a>(
        prepared: &Self::Prepared<'a>,
        index: &I::Index<'a>,
        label: &'static str,
        default: OnMissing,
    ) -> QueryResult<Option<Self::Value>>
    where
        Self: 'a,
    {
        match Self::lookup(prepared, index) {
            Looked::Present(wrapped) => wrapped.clone().map(Some),
            Looked::Absent(absent) => match default {
                OnMissing::Drop => Ok(None),
                OnMissing::Raise => Err(Failure::new(label, absent).at(index).help(
                    "the argument has no value at this index; supply `on_missing(Drop)` or `on_missing(Replace(...))`",
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

impl<I: IndexDomain> ArgumentSource<I> for GraphRecordValue {
    type Value = Self;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _index: &I::Index<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value>>
    where
        Self: 'a,
    {
        Looked::Present(prepared)
    }
}
