use crate::{ExpandedIndexOwned, ExpandedIndexReference, IndexDomain, Position};
use graphrecords_core::graphrecord::{
    AttributeName, AttributeNameView, Group, GroupView, NodeIndex, NodeIndexView, Value, ValueView,
};

pub trait EnsureSortable: PartialOrd + Sized {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a;
}

pub fn incomparable_with_first<'a, V: PartialOrd + 'a>(
    mut values: impl Iterator<Item = &'a V>,
) -> Option<(usize, usize)> {
    let first = values.next()?;

    values
        .position(|value| value.partial_cmp(first).is_none())
        .map(|position| (0, position + 1))
}

pub fn incomparable_pair<'a, V: PartialOrd + 'a>(
    values: impl Iterator<Item = &'a V>,
) -> Option<(usize, usize)> {
    let values: Vec<_> = values.collect();

    for first_position in 0..values.len() {
        for second_position in (first_position + 1)..values.len() {
            if values[first_position]
                .partial_cmp(values[second_position])
                .is_none()
            {
                return Some((first_position, second_position));
            }
        }
    }

    None
}

impl<T: EnsureSortable> EnsureSortable for &T {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a,
    {
        T::find_incomparable(values.copied())
    }
}

impl EnsureSortable for Value {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        incomparable_with_first(values)
    }
}

impl EnsureSortable for ValueView<'_> {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a,
    {
        incomparable_with_first(values)
    }
}

impl EnsureSortable for NodeIndex {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        incomparable_with_first(values)
    }
}

impl EnsureSortable for NodeIndexView<'_> {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a,
    {
        incomparable_with_first(values)
    }
}

impl EnsureSortable for AttributeName {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        incomparable_with_first(values)
    }
}

impl EnsureSortable for AttributeNameView<'_> {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a,
    {
        incomparable_with_first(values)
    }
}

impl EnsureSortable for Group {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        incomparable_with_first(values)
    }
}

impl EnsureSortable for GroupView<'_> {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a,
    {
        incomparable_with_first(values)
    }
}

impl EnsureSortable for bool {
    fn find_incomparable<'a>(_values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        None
    }
}

impl EnsureSortable for Position {
    fn find_incomparable<'a>(_values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        None
    }
}

impl<P, C> EnsureSortable for ExpandedIndexOwned<P, C>
where
    P: IndexDomain,
    C: IndexDomain,
    P::Owned: EnsureSortable,
    C::Owned: EnsureSortable,
{
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a,
    {
        incomparable_pair(values)
    }
}

impl<'index, P, C> EnsureSortable for ExpandedIndexReference<'index, P, C>
where
    P: IndexDomain,
    C: IndexDomain,
    P::Index<'index>: EnsureSortable,
    C::Index<'index>: EnsureSortable,
{
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a,
    {
        incomparable_pair(values)
    }
}
