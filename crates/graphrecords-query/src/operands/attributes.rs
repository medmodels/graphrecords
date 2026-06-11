use super::OperandHandle;
use crate::{
    AttributeName, AttributeSet, Bare, BoxedIterator, EvaluateOperand, IndexDomain, Indexed,
    Multiple, Single,
    error::QueryResult,
    execution::EvaluationCache,
    operations::{Absent, ArgumentSource, Looked, Prepare},
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

impl<I: IndexDomain> ArgumentSource<I> for NestedAttributesOperand<I> {
    type Value = GrHashSet<GraphRecordAttribute>;

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

impl<I: IndexDomain> ArgumentSource<I> for AttributesOperand<I> {
    type Value = GraphRecordAttribute;

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

impl<I1: IndexDomain, I2: IndexDomain> ArgumentSource<I1> for AttributeOperand<I2> {
    type Value = GraphRecordAttribute;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _index: &I1::Index<'a>,
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

impl<I: IndexDomain> ArgumentSource<I> for BareAttributeOperand {
    type Value = GraphRecordAttribute;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _index: &I::Index<'a>,
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
