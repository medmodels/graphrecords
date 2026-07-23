use super::OperandHandle;
use crate::{
    Bare, EvaluateOperand, Failure, FailureKind, FailureKindValue, FailureValue, IndexDomain,
    Indexed, Multiple, OrderState, QueryResult, Single,
    execution::EvaluationCache,
    operations::{Absent, Alignment, ArgumentSource, Keyed, Lookup, Prepare, Preserving},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;
use std::sync::Arc;

pub type FailuresOperand<I, O> = OperandHandle<Indexed<I, FailureValue>, Multiple<O>>;
pub type FailureKindsOperand<I, O> = OperandHandle<Indexed<I, FailureKindValue>, Multiple<O>>;
pub type BareFailuresOperand<O> = OperandHandle<Bare<FailureValue>, Multiple<O>>;
pub type BareFailureKindsOperand<O> = OperandHandle<Bare<FailureKindValue>, Multiple<O>>;
pub type FailureOperand<I> = OperandHandle<Indexed<I, FailureValue>, Single>;
pub type FailureKindOperand<I> = OperandHandle<Indexed<I, FailureKindValue>, Single>;
pub type BareFailureOperand = OperandHandle<Bare<FailureValue>, Single>;
pub type BareFailureKindOperand = OperandHandle<Bare<FailureKindValue>, Single>;

impl<I: IndexDomain, O: OrderState> Prepare for FailuresOperand<I, O> {
    type Prepared<'a> = Arc<GrHashMap<I::Index<'a>, QueryResult<Failure>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain, O: OrderState> ArgumentSource<Keyed<I>> for FailuresOperand<I, O> {
    type Retention = Preserving;
    type Value<'a> = Failure;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        index: &I::Index<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared.get(index) {
            Some(wrapped) => Lookup::Present(wrapped),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

impl<I: IndexDomain, O: OrderState> Prepare for FailureKindsOperand<I, O> {
    type Prepared<'a> = Arc<GrHashMap<I::Index<'a>, QueryResult<FailureKind>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain, O: OrderState> ArgumentSource<Keyed<I>> for FailureKindsOperand<I, O> {
    type Retention = Preserving;
    type Value<'a> = FailureKind;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        index: &I::Index<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared.get(index) {
            Some(wrapped) => Lookup::Present(wrapped),
            None => Lookup::Absent(Absent::Uncovered),
        }
    }
}

impl<I: IndexDomain> Prepare for FailureOperand<I> {
    type Prepared<'a> = Option<QueryResult<Failure>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self
            .evaluate(graphrecord, cache)?
            .map(|(_, failure)| failure))
    }
}

impl<A: Alignment, I: IndexDomain> ArgumentSource<A> for FailureOperand<I> {
    type Retention = Preserving;
    type Value<'a> = Failure;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(failure) => Lookup::Present(failure),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl<I: IndexDomain> Prepare for FailureKindOperand<I> {
    type Prepared<'a> = Option<QueryResult<FailureKind>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.evaluate(graphrecord, cache)?.map(|(_, kind)| kind))
    }
}

impl<A: Alignment, I: IndexDomain> ArgumentSource<A> for FailureKindOperand<I> {
    type Retention = Preserving;
    type Value<'a> = FailureKind;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(kind) => Lookup::Present(kind),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl Prepare for BareFailureOperand {
    type Prepared<'a> = Option<QueryResult<Failure>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment> ArgumentSource<A> for BareFailureOperand {
    type Retention = Preserving;
    type Value<'a> = Failure;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(failure) => Lookup::Present(failure),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl Prepare for BareFailureKindOperand {
    type Prepared<'a> = Option<QueryResult<FailureKind>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment> ArgumentSource<A> for BareFailureKindOperand {
    type Retention = Preserving;
    type Value<'a> = FailureKind;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(kind) => Lookup::Present(kind),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}
