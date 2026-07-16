mod first;
mod last;
mod sort;
mod sort_by;
mod unorder;

use crate::{Diagnostic, OwnedIndex, Position};
pub use first::FirstOperation;
use graphrecords_core::graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue};
pub use last::LastOperation;
pub use sort::SortOperation;
pub use sort_by::SortByOperation;
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};
pub use unorder::UnorderOperation;

pub fn incomparable_with_first<'a, V: PartialOrd + 'a>(
    mut values: impl Iterator<Item = &'a V>,
) -> Option<(usize, usize)> {
    let first = values.next()?;

    values
        .position(|value| value.partial_cmp(first).is_none())
        .map(|position| (0, position + 1))
}

pub trait EnsureSortable: PartialOrd + Sized {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a;
}

impl<T: EnsureSortable> EnsureSortable for &T {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)>
    where
        Self: 'a,
    {
        T::find_incomparable(values.copied())
    }
}

impl EnsureSortable for GraphRecordValue {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        incomparable_with_first(values)
    }
}

impl EnsureSortable for GraphRecordAttribute {
    fn find_incomparable<'a>(values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
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

impl EnsureSortable for EdgeIndex {
    fn find_incomparable<'a>(_values: impl Iterator<Item = &'a Self>) -> Option<(usize, usize)> {
        None
    }
}

#[derive(Debug)]
pub struct IncomparableIndices<V, E: OwnedIndex> {
    pub value: V,
    pub first: E,
    pub second: E,
}

impl<V: Display, E: OwnedIndex> Display for IncomparableIndices<V, E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot order elements sharing value `{}`: their indices `{}` and `{}` are not comparable",
            self.value, self.first, self.second
        )
    }
}

impl<V: Debug + Display, E: OwnedIndex> Error for IncomparableIndices<V, E> {}

impl<V: Debug + Display + Send + Sync + 'static, E: OwnedIndex> Diagnostic
    for IncomparableIndices<V, E>
{
    fn help(&self) -> Option<String> {
        Some(
            "to order them deterministically, sort by a key that distinguishes these elements"
                .to_string(),
        )
    }
}
