use crate::{BoxedIterator, QueryResult};
use elsa::FrozenMap;
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue};
use std::{
    any::{Any, TypeId},
    marker::PhantomData,
};

pub trait Cacheable<'a>: Sized {
    type Owned: 'static;

    fn into_owned(self) -> Self::Owned;

    fn from_owned(owned: &'a Self::Owned) -> Self;
}

macro_rules! cacheable_owned_leaf {
    ($Type:ty) => {
        impl<'a> Cacheable<'a> for $Type {
            type Owned = $Type;

            fn into_owned(self) -> Self::Owned {
                self
            }

            fn from_owned(owned: &'a Self::Owned) -> Self {
                owned.clone()
            }
        }
    };
}

macro_rules! cacheable_tuple {
    ($($F:ident),+) => {
        impl<'a, $($F: Cacheable<'a>),+> Cacheable<'a> for ($($F,)+) {
            type Owned = ($($F::Owned,)+);

            #[allow(non_snake_case)]
            fn into_owned(self) -> Self::Owned {
                let ($($F,)+) = self;

                ($($F.into_owned(),)+)
            }

            #[allow(non_snake_case)]
            fn from_owned(owned: &'a Self::Owned) -> Self {
                let ($($F,)+) = owned;

                ($($F::from_owned($F),)+)
            }
        }
    };
}

impl<'a, T: Clone + 'static> Cacheable<'a> for &'a T {
    type Owned = T;

    fn into_owned(self) -> Self::Owned {
        self.clone()
    }

    fn from_owned(owned: &'a Self::Owned) -> Self {
        owned
    }
}

impl<'a, I> Cacheable<'a> for BoxedIterator<'a, I>
where
    I: Cacheable<'a> + 'a,
{
    type Owned = Vec<I::Owned>;

    fn into_owned(self) -> Self::Owned {
        self.map(I::into_owned).collect()
    }

    fn from_owned(owned: &'a Self::Owned) -> Self {
        Box::new(owned.iter().map(I::from_owned))
    }
}

impl<'a, I> Cacheable<'a> for Option<I>
where
    I: Cacheable<'a>,
{
    type Owned = Option<I::Owned>;

    fn into_owned(self) -> Self::Owned {
        self.map(I::into_owned)
    }

    fn from_owned(owned: &'a Self::Owned) -> Self {
        owned.as_ref().map(I::from_owned)
    }
}

impl<'a, T> Cacheable<'a> for QueryResult<T>
where
    T: Cacheable<'a>,
{
    type Owned = QueryResult<T::Owned>;

    fn into_owned(self) -> Self::Owned {
        self.map(T::into_owned)
    }

    fn from_owned(owned: &'a Self::Owned) -> Self {
        match owned {
            Ok(value) => Ok(T::from_owned(value)),
            Err(failure) => Err(failure.clone()),
        }
    }
}

cacheable_owned_leaf!(bool);
cacheable_owned_leaf!(usize);
cacheable_owned_leaf!(EdgeIndex);
cacheable_owned_leaf!(GraphRecordValue);
cacheable_owned_leaf!(GraphRecordAttribute);
cacheable_owned_leaf!(Vec<GraphRecordAttribute>);

cacheable_tuple!(A, B);
cacheable_tuple!(A, B, C);
cacheable_tuple!(A, B, C, D);
cacheable_tuple!(A, B, C, D, E);
cacheable_tuple!(A, B, C, D, E, F);
cacheable_tuple!(A, B, C, D, E, F, G);
cacheable_tuple!(A, B, C, D, E, F, G, H);
cacheable_tuple!(A, B, C, D, E, F, G, H, I);
cacheable_tuple!(A, B, C, D, E, F, G, H, I, J);
cacheable_tuple!(A, B, C, D, E, F, G, H, I, J, K);
cacheable_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);

pub struct EvaluationCache<'a> {
    cache: FrozenMap<(u64, TypeId), Box<dyn Any>>,
    marker: PhantomData<&'a ()>,
}

impl Default for EvaluationCache<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> EvaluationCache<'a> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: FrozenMap::new(),
            marker: PhantomData,
        }
    }

    pub fn materialize<V>(
        &'a self,
        key: u64,
        compute: impl FnOnce() -> QueryResult<V>,
    ) -> QueryResult<V>
    where
        V: Cacheable<'a>,
    {
        let identifier = (key, TypeId::of::<V::Owned>());

        let stored = match self.cache.get(&identifier) {
            Some(stored) => stored,
            None => self
                .cache
                .insert(identifier, Box::new(compute()?.into_owned())),
        };

        #[allow(clippy::missing_panics_doc)]
        Ok(V::from_owned(
            stored
                .downcast_ref()
                .expect("Cache entry type must match its TypeId key"),
        ))
    }
}
