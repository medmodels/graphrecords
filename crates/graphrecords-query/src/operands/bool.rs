use super::OperandHandle;
use crate::{
    Bare, BoxedIterator, Definite, EvaluateOperand, IndexDomain, Indexed, Mask, MaskMap, Multiple,
    OrderState, Single,
    error::QueryResult,
    execution::EvaluationCache,
    operations::{Absent, Alignment, ArgumentSource, Keyed, Lookup, Prepare, Preserving},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashMap;
use std::sync::Arc;

pub type NestedBoolMaskIterator<'a, I, T> = BoxedIterator<'a, (<I as IndexDomain>::Index<'a>, T)>;

pub type NestedBoolMaskOperand<I, T, O> = OperandHandle<Indexed<I, MaskMap<T>>, Multiple<O>>;
pub type BoolMaskOperand<I, O> = OperandHandle<Indexed<I, Mask>, Multiple<O>>;
pub type BoolOperand<I> = OperandHandle<Indexed<I, Mask>, Single>;
pub type BareBoolMaskOperand<O> = OperandHandle<Bare<Mask>, Multiple<O>>;
pub type BareBoolOperand = OperandHandle<Bare<Mask>, Single>;
pub type DefiniteBoolOperand = OperandHandle<Bare<Mask>, Definite>;

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
    type Retention = Preserving;
    type Value<'a> = GrHashMap<T, bool>;

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
    type Retention = Preserving;
    type Value<'a> = bool;

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

impl<I: IndexDomain> Prepare for BoolOperand<I> {
    type Prepared<'a> = Option<QueryResult<bool>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self.evaluate(graphrecord, cache)?.map(|(_, value)| value))
    }
}

impl<A: Alignment, I: IndexDomain> ArgumentSource<A> for BoolOperand<I> {
    type Retention = Preserving;
    type Value<'a> = bool;

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

impl Prepare for BareBoolOperand {
    type Prepared<'a> = Option<QueryResult<bool>>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment> ArgumentSource<A> for BareBoolOperand {
    type Retention = Preserving;
    type Value<'a> = bool;

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

impl Prepare for DefiniteBoolOperand {
    type Prepared<'a> = QueryResult<bool>;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.evaluate(graphrecord, cache)
    }
}

impl<A: Alignment> ArgumentSource<A> for DefiniteBoolOperand {
    type Retention = Preserving;
    type Value<'a> = bool;

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
