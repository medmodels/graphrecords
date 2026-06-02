use crate::BoxedIterator;
use elsa::FrozenMap;
use graphrecords_core::{
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue},
};
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
    ($($Field:ident),+) => {
        impl<'a, $($Field: Cacheable<'a>),+> Cacheable<'a> for ($($Field,)+) {
            type Owned = ($($Field::Owned,)+);

            #[allow(non_snake_case)]
            fn into_owned(self) -> Self::Owned {
                let ($($Field,)+) = self;

                ($($Field.into_owned(),)+)
            }

            #[allow(non_snake_case)]
            fn from_owned(owned: &'a Self::Owned) -> Self {
                let ($($Field,)+) = owned;

                ($($Field::from_owned($Field),)+)
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

impl<'a, Item> Cacheable<'a> for BoxedIterator<'a, Item>
where
    Item: Cacheable<'a> + 'a,
{
    type Owned = Vec<Item::Owned>;

    fn into_owned(self) -> Self::Owned {
        self.map(Item::into_owned).collect()
    }

    fn from_owned(owned: &'a Self::Owned) -> Self {
        Box::new(owned.iter().map(Item::from_owned))
    }
}

impl<'a, Item> Cacheable<'a> for Option<Item>
where
    Item: Cacheable<'a>,
{
    type Owned = Option<Item::Owned>;

    fn into_owned(self) -> Self::Owned {
        self.map(Item::into_owned)
    }

    fn from_owned(owned: &'a Self::Owned) -> Self {
        owned.as_ref().map(Item::from_owned)
    }
}

cacheable_owned_leaf!(bool);
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

pub struct ExecutionContext<'a> {
    cache: FrozenMap<(u64, TypeId), Box<dyn Any>>,
    marker: PhantomData<&'a ()>,
}

impl Default for ExecutionContext<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> ExecutionContext<'a> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: FrozenMap::new(),
            marker: PhantomData,
        }
    }

    /// # Panics
    ///
    /// Panics if a cache entry's stored type does not match its `TypeId` key.
    pub fn materialize<Value>(
        &'a self,
        key: u64,
        compute: impl FnOnce() -> GraphRecordResult<Value>,
    ) -> GraphRecordResult<Value>
    where
        Value: Cacheable<'a>,
    {
        let identifier = (key, TypeId::of::<Value::Owned>());

        let stored = match self.cache.get(&identifier) {
            Some(stored) => stored,
            None => self
                .cache
                .insert(identifier, Box::new(compute()?.into_owned())),
        };

        Ok(Value::from_owned(
            stored
                .downcast_ref()
                .expect("Cache entry type must match its TypeId key"),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use graphrecords_core::graphrecord::NodeIndex;
    use std::cell::Cell;

    #[test]
    fn test_materialize_replays_cached_values_without_recomputing() {
        let context = ExecutionContext::new();
        let lorem: NodeIndex = "lorem".into();
        let ipsum: NodeIndex = "ipsum".into();
        let computations = Cell::new(0u32);

        let initial = context
            .materialize(0, || {
                computations.set(computations.get() + 1);

                Ok(Box::new(
                    [
                        (&lorem, GraphRecordValue::from("amet")),
                        (&ipsum, GraphRecordValue::from("consectetur")),
                    ]
                    .into_iter(),
                )
                    as BoxedIterator<'_, (&NodeIndex, GraphRecordValue)>)
            })
            .unwrap()
            .map(|(index, value)| (index.clone(), value))
            .collect::<Vec<_>>();

        // The cached entry must replay even though this closure would compute different values.
        let replayed = context
            .materialize(0, || {
                computations.set(computations.get() + 1);

                Ok(Box::new(std::iter::empty())
                    as BoxedIterator<'_, (&NodeIndex, GraphRecordValue)>)
            })
            .unwrap()
            .map(|(index, value)| (index.clone(), value))
            .collect::<Vec<_>>();

        assert_eq!(1, computations.get());
        assert_eq!(
            vec![
                (NodeIndex::from("lorem"), GraphRecordValue::from("amet")),
                (
                    NodeIndex::from("ipsum"),
                    GraphRecordValue::from("consectetur")
                ),
            ],
            initial
        );
        assert_eq!(initial, replayed);
    }

    #[test]
    fn test_materialize_separates_distinct_keys() {
        let context = ExecutionContext::new();
        let lorem: NodeIndex = "lorem".into();

        let single = context
            .materialize(0, || {
                Ok(
                    Box::new(std::iter::once((&lorem, GraphRecordValue::from("amet"))))
                        as BoxedIterator<'_, (&NodeIndex, GraphRecordValue)>,
                )
            })
            .unwrap()
            .count();

        let pair = context
            .materialize(1, || {
                Ok(Box::new(
                    [
                        (&lorem, GraphRecordValue::from("amet")),
                        (&lorem, GraphRecordValue::from("consectetur")),
                    ]
                    .into_iter(),
                )
                    as BoxedIterator<'_, (&NodeIndex, GraphRecordValue)>)
            })
            .unwrap()
            .count();

        assert_eq!(1, single);
        assert_eq!(2, pair);
    }
}
