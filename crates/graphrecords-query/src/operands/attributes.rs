use super::OperandHandle;
use crate::{
    AttributeName, AttributeSet, Bare, BoxedIterator, EvaluateOperand, IndexDomain, Indexed,
    Multiple, Single,
    error::QueryResult,
    execution::EvaluationCache,
    operations::{Absent, Alignment, ArgumentSource, Keyed, Looked, Prepare},
};
use graphrecords_core::{GraphRecord, graphrecord::GraphRecordAttribute};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::sync::Arc;

pub type NestedAttributesIterator<'a, I, T> = BoxedIterator<'a, (<I as IndexDomain>::Index<'a>, T)>;

pub type NestedAttributesOperand<I> = OperandHandle<Indexed<I, AttributeSet>, Multiple>;
pub type AttributesOperand<I> = OperandHandle<Indexed<I, AttributeName>, Multiple>;
pub type BareAttributesOperand = OperandHandle<Bare<AttributeName>, Multiple>;
pub type AttributeOperand<I> = OperandHandle<Indexed<I, AttributeName>, Single>;
pub type BareAttributeOperand = OperandHandle<Bare<AttributeName>, Single>;

impl<I: IndexDomain> Prepare for NestedAttributesOperand<I> {
    type Prepared<'a> = Arc<GrHashMap<I::Index<'a>, QueryResult<GrHashSet<GraphRecordAttribute>>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain> ArgumentSource<Keyed<I>> for NestedAttributesOperand<I> {
    type Value<'a> = GrHashSet<GraphRecordAttribute>;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        index: &I::Index<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared.get(index) {
            Some(wrapped) => Looked::Present(wrapped),
            None => Looked::Absent(Absent::Uncovered),
        }
    }
}

impl<I: IndexDomain> Prepare for AttributesOperand<I> {
    type Prepared<'a> = Arc<GrHashMap<I::Index<'a>, QueryResult<GraphRecordAttribute>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain> ArgumentSource<Keyed<I>> for AttributesOperand<I> {
    type Value<'a> = GraphRecordAttribute;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        index: &I::Index<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared.get(index) {
            Some(wrapped) => Looked::Present(wrapped),
            None => Looked::Absent(Absent::Uncovered),
        }
    }
}

impl<I: IndexDomain> Prepare for AttributeOperand<I> {
    type Prepared<'a> = Option<QueryResult<GraphRecordAttribute>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.evaluate(graphrecord, cache)?.map(|(_, value)| value))
    }
}

impl<A: Alignment, I: IndexDomain> ArgumentSource<A> for AttributeOperand<I> {
    type Value<'a> = GraphRecordAttribute;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(wrapped) => Looked::Present(wrapped),
            None => Looked::Absent(Absent::Empty),
        }
    }
}

impl Prepare for BareAttributeOperand {
    type Prepared<'a> = Option<QueryResult<GraphRecordAttribute>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment> ArgumentSource<A> for BareAttributeOperand {
    type Value<'a> = GraphRecordAttribute;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        match prepared {
            Some(wrapped) => Looked::Present(wrapped),
            None => Looked::Absent(Absent::Empty),
        }
    }
}
