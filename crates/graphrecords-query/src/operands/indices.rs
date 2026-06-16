use super::OperandHandle;
use crate::{
    EvaluateOperand, IndexDomain, IndexValue, Indexed, Multiple, Single,
    error::QueryResult,
    execution::EvaluationCache,
    operations::{Absent, Alignment, ArgumentSource, Keyed, Looked, Prepare},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;
use std::sync::Arc;

pub type IndicesOperand<I> = OperandHandle<Indexed<I, IndexValue<I>>, Multiple>;
pub type IndexOperand<I> = OperandHandle<Indexed<I, IndexValue<I>>, Single>;

impl<I: IndexDomain> Prepare for IndicesOperand<I> {
    type Prepared<'a> = Arc<GrHashMap<I::Index<'a>, QueryResult<I>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain> ArgumentSource<Keyed<I>> for IndicesOperand<I> {
    type Value = I;

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

impl<I: IndexDomain> Prepare for IndexOperand<I> {
    type Prepared<'a> = Option<QueryResult<I>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.evaluate(graphrecord, cache)?.map(|(_, value)| value))
    }
}

impl<A: Alignment, I: IndexDomain> ArgumentSource<A> for IndexOperand<I> {
    type Value = I;

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
