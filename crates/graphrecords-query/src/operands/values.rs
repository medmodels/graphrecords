use super::OperandHandle;
use crate::{
    Bare, Definite, EvaluateOperand, IndexDomain, Indexed, Multiple, OrderState, Scalar, Single,
    error::QueryResult,
    execution::EvaluationCache,
    operations::{Absent, Alignment, ArgumentSource, Keyed, Lookup, Prepare, Preserving},
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use graphrecords_utils::aliases::GrHashMap;
use std::sync::Arc;

pub type ValuesOperand<I, O> = OperandHandle<Indexed<I, Scalar>, Multiple<O>>;
pub type BareValuesOperand<O> = OperandHandle<Bare<Scalar>, Multiple<O>>;
pub type ValueOperand<I> = OperandHandle<Indexed<I, Scalar>, Single>;
pub type BareValueOperand = OperandHandle<Bare<Scalar>, Single>;
pub type DefiniteValueOperand = OperandHandle<Bare<Scalar>, Definite>;

impl<I: IndexDomain, O: OrderState> Prepare for ValuesOperand<I, O> {
    type Prepared<'a> = Arc<GrHashMap<I::Index<'a>, QueryResult<GraphRecordValue>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain, O: OrderState> ArgumentSource<Keyed<I>> for ValuesOperand<I, O> {
    type Retention = Preserving;
    type Value<'a> = GraphRecordValue;

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

impl<I: IndexDomain> Prepare for ValueOperand<I> {
    type Prepared<'a> = Option<QueryResult<GraphRecordValue>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.evaluate(graphrecord, cache)?.map(|(_, value)| value))
    }
}

impl<A: Alignment, I: IndexDomain> ArgumentSource<A> for ValueOperand<I> {
    type Retention = Preserving;
    type Value<'a> = GraphRecordValue;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(wrapped) => Lookup::Present(wrapped),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl Prepare for BareValueOperand {
    type Prepared<'a> = Option<QueryResult<GraphRecordValue>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment> ArgumentSource<A> for BareValueOperand {
    type Retention = Preserving;
    type Value<'a> = GraphRecordValue;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Lookup<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(wrapped) => Lookup::Present(wrapped),
            None => Lookup::Absent(Absent::Empty),
        }
    }
}

impl Prepare for DefiniteValueOperand {
    type Prepared<'a> = QueryResult<GraphRecordValue>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment> ArgumentSource<A> for DefiniteValueOperand {
    type Retention = Preserving;
    type Value<'a> = GraphRecordValue;

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
