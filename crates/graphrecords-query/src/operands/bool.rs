use super::OperandHandle;
use crate::{
    BoxedIterator, Definite, EvaluateOperand, IndexDomain, Indexed, Mask, MaskMap, Multiple,
    OrderState,
    error::QueryResult,
    execution::EvaluationCache,
    operations::{Absent, Alignment, ArgumentSource, Keyed, Looked, Prepare},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;
use std::sync::Arc;

pub type NestedBoolMaskIterator<'a, I, T> = BoxedIterator<'a, (<I as IndexDomain>::Index<'a>, T)>;

pub type NestedBoolMaskOperand<I, T, O> = OperandHandle<Indexed<I, MaskMap<T>>, Multiple<O>>;
pub type BoolMaskOperand<I, O> = OperandHandle<Indexed<I, Mask>, Multiple<O>>;
pub type BoolOperand<I> = OperandHandle<Indexed<I, Mask>, Definite>;

impl<I: IndexDomain, T: 'static + Clone, O: OrderState> Prepare for NestedBoolMaskOperand<I, T, O> {
    type Prepared<'a> = Arc<GrHashMap<I::Index<'a>, QueryResult<GrHashMap<T, bool>>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain, T: 'static + Clone, O: OrderState> ArgumentSource<Keyed<I>>
    for NestedBoolMaskOperand<I, T, O>
{
    type Value<'a> = GrHashMap<T, bool>;

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

impl<I: IndexDomain, O: OrderState> Prepare for BoolMaskOperand<I, O> {
    type Prepared<'a> = Arc<GrHashMap<I::Index<'a>, QueryResult<bool>>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(Arc::new(self.evaluate(graphrecord, cache)?.collect()))
    }
}

impl<I: IndexDomain, O: OrderState> ArgumentSource<Keyed<I>> for BoolMaskOperand<I, O> {
    type Value<'a> = bool;

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

impl<I: IndexDomain> Prepare for BoolOperand<I> {
    type Prepared<'a> = (I::Index<'a>, QueryResult<bool>);

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment, I: IndexDomain> ArgumentSource<A> for BoolOperand<I> {
    type Value<'a> = bool;

    fn lookup<'a, 'prepared>(
        prepared: &'prepared Self::Prepared<'a>,
        _address: &A::Address<'a>,
    ) -> Looked<'prepared, QueryResult<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Looked::Present(&prepared.1)
    }
}
