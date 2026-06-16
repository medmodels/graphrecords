use super::OperandHandle;
use crate::{
    Bare, EvaluateOperand, IndexDomain, Indexed, Multiple, Scalar, Single,
    error::QueryResult,
    execution::EvaluationCache,
    operations::{Absent, Alignment, ArgumentSource, Keyed, Looked, Prepare},
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
use graphrecords_utils::aliases::GrHashMap;
use std::sync::Arc;

pub type ValuesOperand<I> = OperandHandle<Indexed<I, Scalar>, Multiple>;
pub type BareValuesOperand = OperandHandle<Bare<Scalar>, Multiple>;
pub type ValueOperand<I> = OperandHandle<Indexed<I, Scalar>, Single>;
pub type BareValueOperand = OperandHandle<Bare<Scalar>, Single>;

impl<I: IndexDomain> Prepare for ValuesOperand<I> {
    type Prepared<'a> = Arc<GrHashMap<I::Index<'a>, QueryResult<GraphRecordValue>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain> ArgumentSource<Keyed<I>> for ValuesOperand<I> {
    type Value = GraphRecordValue;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        index: &I::Index<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value>>
    where
        Self: 'a,
    {
        match prepared.get(index) {
            Some(wrapped) => Looked::Present(wrapped),
            None => Looked::Absent(Absent::Uncovered),
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
    type Value = GraphRecordValue;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value>>
    where
        Self: 'a,
    {
        match prepared {
            Some(wrapped) => Looked::Present(wrapped),
            None => Looked::Absent(Absent::Empty),
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
    type Value = GraphRecordValue;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value>>
    where
        Self: 'a,
    {
        match prepared {
            Some(wrapped) => Looked::Present(wrapped),
            None => Looked::Absent(Absent::Empty),
        }
    }
}
