use super::OperandHandle;
use crate::{
    EvaluateOperand, IndexDomain, IndexValue, Indexed, Multiple, OrderState, Single,
    error::QueryResult,
    execution::EvaluationCache,
    operations::{Absent, Alignment, ArgumentSource, Keyed, Lookup, Prepare, Preserving},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;
use std::sync::Arc;

pub type IndicesOperand<I, O> = OperandHandle<Indexed<I, IndexValue<I>>, Multiple<O>>;
pub type IndexOperand<I> = OperandHandle<Indexed<I, IndexValue<I>>, Single>;
pub type ReferenceOperand<K, E, O> = OperandHandle<Indexed<K, IndexValue<E>>, Multiple<O>>;

impl<K: IndexDomain, E: IndexDomain, O: OrderState> Prepare for ReferenceOperand<K, E, O> {
    type Prepared<'a> = Arc<GrHashMap<K::Index<'a>, QueryResult<E::Index<'a>>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<K: IndexDomain, E: IndexDomain, O: OrderState> ArgumentSource<Keyed<K>>
    for ReferenceOperand<K, E, O>
{
    type Retention = Preserving;
    type Value<'a> = E::Index<'a>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        index: &K::Index<'a>,
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

impl<I: IndexDomain> Prepare for IndexOperand<I> {
    type Prepared<'a> = Option<QueryResult<I::Index<'a>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.evaluate(graphrecord, cache)?.map(|(_, value)| value))
    }
}

impl<A: Alignment, I: IndexDomain> ArgumentSource<A> for IndexOperand<I> {
    type Retention = Preserving;
    type Value<'a> = I::Index<'a>;

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
