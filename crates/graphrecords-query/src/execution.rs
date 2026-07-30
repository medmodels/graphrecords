use crate::{
    Arity, Bare, Definite, ElementShape, IndexDomain, Indexed, Multiple, Operand, OrderState,
    QueryResult, Single, ValueType, operands::OperandHandle,
};
use elsa::FrozenMap;
use graphrecords_core::GraphRecord;
use std::{
    any::Any,
    hash::{Hash, Hasher},
    ptr,
    sync::Arc,
};

pub trait CacheableShape: ElementShape {
    type CachedElement: 'static;

    fn into_cached_element(element: Self::Element<'_>) -> Self::CachedElement;

    fn from_cached_element(cached: &Self::CachedElement) -> Self::Element<'_>;
}

pub trait CacheableArity<S: CacheableShape>: Arity {
    type Cached: 'static;

    fn into_cached<'a>(values: Self::Container<'a, S::Element<'a>>) -> Self::Cached;

    fn from_cached(cached: &Self::Cached) -> Self::Container<'_, S::Element<'_>>;
}

pub trait CacheableOperand: Operand {
    type Cached: 'static;

    fn into_cached(values: Self::ReturnValue<'_>) -> Self::Cached;

    fn from_cached(cached: &Self::Cached) -> Self::ReturnValue<'_>;
}

impl<I: IndexDomain, V: ValueType> CacheableShape for Indexed<I, V> {
    type CachedElement = (I::Owned, QueryResult<V::Owned>);

    fn into_cached_element(element: Self::Element<'_>) -> Self::CachedElement {
        let (index, outcome) = element;

        (I::to_owned(&index), outcome.map(V::into_owned))
    }

    fn from_cached_element(cached: &Self::CachedElement) -> Self::Element<'_> {
        let outcome = match &cached.1 {
            Ok(value) => Ok(V::from_owned(value)),
            Err(failure) => Err(failure.clone()),
        };

        (I::from_owned(&cached.0), outcome)
    }
}

impl<V: ValueType> CacheableShape for Bare<V> {
    type CachedElement = QueryResult<V::Owned>;

    fn into_cached_element(element: Self::Element<'_>) -> Self::CachedElement {
        element.map(V::into_owned)
    }

    fn from_cached_element(cached: &Self::CachedElement) -> Self::Element<'_> {
        match cached {
            Ok(value) => Ok(V::from_owned(value)),
            Err(failure) => Err(failure.clone()),
        }
    }
}

impl<S: CacheableShape> CacheableArity<S> for Definite {
    type Cached = S::CachedElement;

    fn into_cached<'a>(values: Self::Container<'a, S::Element<'a>>) -> Self::Cached {
        S::into_cached_element(values)
    }

    fn from_cached(cached: &Self::Cached) -> Self::Container<'_, S::Element<'_>> {
        S::from_cached_element(cached)
    }
}

impl<S: CacheableShape> CacheableArity<S> for Single {
    type Cached = Option<S::CachedElement>;

    fn into_cached<'a>(values: Self::Container<'a, S::Element<'a>>) -> Self::Cached {
        values.map(S::into_cached_element)
    }

    fn from_cached(cached: &Self::Cached) -> Self::Container<'_, S::Element<'_>> {
        cached.as_ref().map(S::from_cached_element)
    }
}

impl<S: CacheableShape, O: OrderState> CacheableArity<S> for Multiple<O> {
    type Cached = Vec<S::CachedElement>;

    fn into_cached<'a>(values: Self::Container<'a, S::Element<'a>>) -> Self::Cached {
        values.map(S::into_cached_element).collect()
    }

    fn from_cached(cached: &Self::Cached) -> Self::Container<'_, S::Element<'_>> {
        Box::new(cached.iter().map(S::from_cached_element))
    }
}

impl<S: CacheableShape, C: CacheableArity<S>> CacheableOperand for OperandHandle<S, C> {
    type Cached = C::Cached;

    fn into_cached(values: Self::ReturnValue<'_>) -> Self::Cached {
        C::into_cached(values)
    }

    fn from_cached(cached: &Self::Cached) -> Self::ReturnValue<'_> {
        C::from_cached(cached)
    }
}

#[derive(Clone)]
pub(crate) struct CacheSlot(Arc<CacheSlotMarker>);

struct CacheSlotMarker;

impl CacheSlot {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self(Arc::new(CacheSlotMarker))
    }
}

impl PartialEq for CacheSlot {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CacheSlot {}

impl Hash for CacheSlot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ptr::hash(Arc::as_ptr(&self.0), state);
    }
}

pub struct EvaluationCache<'a> {
    graphrecord: &'a GraphRecord,
    values: FrozenMap<CacheSlot, Box<dyn Any>>,
}

impl<'a> EvaluationCache<'a> {
    #[must_use]
    pub fn new(graphrecord: &'a GraphRecord) -> Self {
        Self {
            graphrecord,
            values: FrozenMap::new(),
        }
    }

    pub(crate) fn is_bound_to(&self, graphrecord: &GraphRecord) -> bool {
        ptr::eq(self.graphrecord, graphrecord)
    }

    pub(crate) fn materialize<O: CacheableOperand>(
        &'a self,
        slot: &CacheSlot,
        compute: impl FnOnce() -> QueryResult<O::ReturnValue<'a>>,
    ) -> QueryResult<O::ReturnValue<'a>> {
        let stored = if let Some(stored) = self.values.get(slot) {
            stored
        } else {
            let computed = Box::new(compute().map(O::into_cached));
            self.values.insert(slot.clone(), computed)
        };

        let stored = stored
            .downcast_ref::<QueryResult<O::Cached>>()
            .expect("Cache entry must match its slot's operand type");

        match stored {
            Ok(cached) => Ok(O::from_cached(cached)),
            Err(failure) => Err(failure.clone()),
        }
    }
}
