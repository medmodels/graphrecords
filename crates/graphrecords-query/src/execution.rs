use crate::{
    Arity, Bare, BareValueDomain, Definite, ElementShape, Expression, IndexDomain, Indexed,
    Multiple, OrderState, QueryResult, Single, ValueDomain, expressions::ExpressionHandle,
};
use elsa::sync::FrozenMap;
use graphrecords_core::{GraphRecord, StateView, graphrecord::StateIdentity};
use std::{
    any::Any,
    hash::{Hash, Hasher},
    ptr,
    sync::Arc,
};

pub trait CacheableShape: ElementShape {
    type CachedElement: 'static + Send + Sync;

    fn into_cached_element(element: Self::Element<'_>) -> Self::CachedElement;

    fn from_cached_element<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::CachedElement,
    ) -> Self::Element<'a>;
}

pub trait CacheableArity<S: CacheableShape>: Arity {
    type Cached: 'static + Send + Sync;

    fn into_cached<'a>(values: Self::Container<'a, S::Element<'a>>) -> Self::Cached;

    fn from_cached<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::Cached,
    ) -> Self::Container<'a, S::Element<'a>>;
}

pub trait CacheableExpression: Expression {
    type Cached: 'static + Send + Sync;

    fn into_cached(values: Self::ReturnValue<'_>) -> Self::Cached;

    fn from_cached<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::Cached,
    ) -> Self::ReturnValue<'a>;
}

impl<I: IndexDomain, V: ValueDomain> CacheableShape for Indexed<I, V> {
    type CachedElement = (I::Address, QueryResult<V::Cached>);

    fn into_cached_element(element: Self::Element<'_>) -> Self::CachedElement {
        let (address, outcome) = element;

        (address, outcome.map(V::into_cached))
    }

    fn from_cached_element<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::CachedElement,
    ) -> Self::Element<'a> {
        let outcome = match &cached.1 {
            Ok(value) => Ok(V::from_cached(graphrecord, value)),
            Err(failure) => Err(failure.clone()),
        };

        (cached.0.clone(), outcome)
    }
}

impl<V: BareValueDomain> CacheableShape for Bare<V> {
    type CachedElement = QueryResult<V::Cached>;

    fn into_cached_element(element: Self::Element<'_>) -> Self::CachedElement {
        element.map(V::into_cached)
    }

    fn from_cached_element<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::CachedElement,
    ) -> Self::Element<'a> {
        match cached {
            Ok(value) => Ok(V::from_cached(graphrecord, value)),
            Err(failure) => Err(failure.clone()),
        }
    }
}

impl<S: CacheableShape, O: OrderState> CacheableArity<S> for Multiple<O> {
    type Cached = Vec<S::CachedElement>;

    fn into_cached<'a>(values: Self::Container<'a, S::Element<'a>>) -> Self::Cached {
        values.map(S::into_cached_element).collect()
    }

    fn from_cached<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::Cached,
    ) -> Self::Container<'a, S::Element<'a>> {
        Box::new(
            cached
                .iter()
                .map(|cached| S::from_cached_element(graphrecord, cached)),
        )
    }
}

impl<S: CacheableShape> CacheableArity<S> for Single {
    type Cached = Option<S::CachedElement>;

    fn into_cached<'a>(values: Self::Container<'a, S::Element<'a>>) -> Self::Cached {
        values.map(S::into_cached_element)
    }

    fn from_cached<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::Cached,
    ) -> Self::Container<'a, S::Element<'a>> {
        cached
            .as_ref()
            .map(|cached| S::from_cached_element(graphrecord, cached))
    }
}

impl<S: CacheableShape> CacheableArity<S> for Definite {
    type Cached = S::CachedElement;

    fn into_cached<'a>(values: Self::Container<'a, S::Element<'a>>) -> Self::Cached {
        S::into_cached_element(values)
    }

    fn from_cached<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::Cached,
    ) -> Self::Container<'a, S::Element<'a>> {
        S::from_cached_element(graphrecord, cached)
    }
}

impl<S: CacheableShape, C: CacheableArity<S>> CacheableExpression for ExpressionHandle<S, C> {
    type Cached = C::Cached;

    fn into_cached(values: Self::ReturnValue<'_>) -> Self::Cached {
        C::into_cached(values)
    }

    fn from_cached<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::Cached,
    ) -> Self::ReturnValue<'a> {
        C::from_cached(graphrecord, cached)
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

pub struct EvaluationCache {
    state_identity: StateIdentity,
    values: FrozenMap<CacheSlot, Box<dyn Any + Send + Sync>>,
}

impl EvaluationCache {
    #[must_use]
    pub fn new(graphrecord: &GraphRecord) -> Self {
        Self {
            state_identity: StateView::of(graphrecord).state_identity(),
            values: FrozenMap::new(),
        }
    }

    pub(crate) fn is_bound_to(&self, graphrecord: &GraphRecord) -> bool {
        self.state_identity == StateView::of(graphrecord).state_identity()
    }

    pub(crate) fn materialize<'a, E: CacheableExpression>(
        &'a self,
        graphrecord: &'a GraphRecord,
        slot: &CacheSlot,
        compute: impl FnOnce() -> QueryResult<E::ReturnValue<'a>>,
    ) -> QueryResult<E::ReturnValue<'a>> {
        let stored = if let Some(stored) = self.values.get(slot) {
            stored
        } else {
            let computed = Box::new(compute().map(E::into_cached));
            self.values.insert(slot.clone(), computed)
        };

        let stored = stored
            .downcast_ref::<QueryResult<E::Cached>>()
            .expect("Cache entry must match its slot's expression type");

        match stored {
            Ok(cached) => Ok(E::from_cached(graphrecord, cached)),
            Err(failure) => Err(failure.clone()),
        }
    }
}
