mod first;
mod last;
mod reverse_order;
mod sample;
mod sample_fraction;
mod shuffle;
mod sort;
mod sort_by;
mod take;
mod unorder;

use crate::{
    EdgeEndpointRole, ExpandedIndex, FailureKind, IndexDomain, Positional,
    capabilities::EnsureSortable, optimizer::Estimate, registry::OperationManifest,
};
pub use first::FirstOperation;
use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeName, EdgeIndex, GroupIndex, NodeIndex, Value},
};
pub use last::LastOperation;
use rand::{Rng, RngExt};
pub use reverse_order::ReverseOrderOperation;
pub use sample::{SampleOperation, SampleWithSeedOperation};
pub use sample_fraction::{SampleFractionOperation, SampleFractionWithSeedOperation};
pub use shuffle::{ShuffleOperation, ShuffleWithSeedOperation};
pub use sort::SortOperation;
pub use sort_by::SortByOperation;
pub use take::{TakeFractionOperation, TakeOperation};
pub use unorder::UnorderOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        first::operation_manifest(),
        last::operation_manifest(),
        reverse_order::operation_manifest(),
        sample::operation_manifest(),
        sample::with_seed::operation_manifest(),
        sample_fraction::operation_manifest(),
        sample_fraction::with_seed::operation_manifest(),
        shuffle::operation_manifest(),
        shuffle::with_seed::operation_manifest(),
        sort::operation_manifest(),
        sort_by::operation_manifest(),
        take::operation_manifest(),
        take::fraction::operation_manifest(),
        unorder::operation_manifest(),
    ]
}

pub(super) fn fraction_elements(fraction: f64, length: usize) -> usize {
    ((fraction * length as f64).round_ties_even() as usize).min(length)
}

pub(super) fn fraction_estimate(fraction: f64, input: Estimate) -> Estimate {
    let elements = input
        .elements
        .map(|elements| fraction_elements(fraction, elements));

    Estimate {
        elements,
        distinct: input
            .distinct
            .zip(elements)
            .map(|(distinct, elements)| distinct.min(elements)),
        selectivity: None,
        ..input
    }
}

pub(super) fn shuffle_prefix<T, G: Rng>(
    mut values: Vec<T>,
    elements: usize,
    generator: &mut G,
) -> Vec<T> {
    let length = values.len();
    let elements = elements.min(length);

    for position in 0..elements {
        let chosen = generator.random_range(position as u64..length as u64);
        values.swap(position, chosen as usize);
    }

    values.truncate(elements);

    values
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

impl IndexTiebreak for GroupIndex {
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
