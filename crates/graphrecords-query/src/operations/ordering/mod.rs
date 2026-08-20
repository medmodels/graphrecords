mod first;
mod last;
mod reverse_order;
mod shuffle;
mod sort;
mod sort_by;
mod take;
mod unorder;

use crate::{
    EdgeEndpointRole, ExpandedIndex, FailureKind, IndexDomain, Positional,
    capabilities::EnsureSortable, registry::OperationManifest,
};
pub use first::FirstOperation;
use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeName, EdgeIndex, Group, NodeIndex, Value},
};
pub use last::LastOperation;
pub use reverse_order::ReverseOrderOperation;
pub use shuffle::ShuffleOperation;
pub use sort::SortOperation;
pub use sort_by::SortByOperation;
pub use take::TakeOperation;
pub use unorder::UnorderOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        first::operation_manifest(),
        last::operation_manifest(),
        reverse_order::operation_manifest(),
        shuffle::operation_manifest(),
        sort::operation_manifest(),
        sort_by::operation_manifest(),
        take::operation_manifest(),
        unorder::operation_manifest(),
    ]
}

fn tiebreak_by_key<I, T, F>(graphrecord: &GraphRecord, run: &mut [T], address: F)
where
    I: IndexDomain,
    F: Fn(&T) -> &I::Address,
    for<'a> I::Index<'a>: EnsureSortable,
{
    let identities: Vec<_> = run
        .iter()
        .map(|element| I::index(graphrecord, address(element)))
        .collect();

    if EnsureSortable::find_incomparable(identities.iter()).is_some() {
        return;
    }

    run.sort_by(|left, right| {
        I::index(graphrecord, address(left))
            .partial_cmp(&I::index(graphrecord, address(right)))
            .unwrap_or_else(|| panic!("EnsureSortable admitted an incomparable pair of identities"))
    });
}

pub trait IndexTiebreak: IndexDomain {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    );
}

impl IndexTiebreak for Positional {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    ) {
        tiebreak_by_key::<Self, _, _>(graphrecord, run, address);
    }
}

impl IndexTiebreak for NodeIndex {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    ) {
        tiebreak_by_key::<Self, _, _>(graphrecord, run, address);
    }
}

impl IndexTiebreak for Group {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    ) {
        tiebreak_by_key::<Self, _, _>(graphrecord, run, address);
    }
}

impl IndexTiebreak for Value {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    ) {
        tiebreak_by_key::<Self, _, _>(graphrecord, run, address);
    }
}

impl IndexTiebreak for AttributeName {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    ) {
        tiebreak_by_key::<Self, _, _>(graphrecord, run, address);
    }
}

impl IndexTiebreak for bool {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    ) {
        tiebreak_by_key::<Self, _, _>(graphrecord, run, address);
    }
}

impl IndexTiebreak for EdgeIndex {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        _graphrecord: &GraphRecord,
        _run: &mut [T],
        _address: F,
    ) {
    }
}

impl IndexTiebreak for FailureKind {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        _graphrecord: &GraphRecord,
        _run: &mut [T],
        _address: F,
    ) {
    }
}

impl IndexTiebreak for EdgeEndpointRole {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        _graphrecord: &GraphRecord,
        _run: &mut [T],
        _address: F,
    ) {
    }
}

impl<P, C> IndexTiebreak for ExpandedIndex<P, C>
where
    P: IndexTiebreak,
    C: IndexTiebreak,
{
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    ) {
        P::tiebreak(graphrecord, run, |element| address(element).parent_index());

        for subrun in run.chunk_by_mut(|left, right| {
            address(left).parent_index() == address(right).parent_index()
        }) {
            if !subrun
                .iter()
                .all(|element| address(element).child_index().is_some())
            {
                continue;
            }

            C::tiebreak(graphrecord, subrun, |element| {
                address(element)
                    .child_index()
                    .expect("subrun was checked to hold only child expanded addresses")
            });
        }
    }
}
