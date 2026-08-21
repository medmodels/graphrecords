mod absolute;
mod ceil;
mod clip;
mod cube_root;
mod exponential;
mod floor;
mod logarithm;
mod negate;
mod round;
mod sign;
mod square_root;

use crate::{
    QueryResult, ValueDomain,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Preserving},
    index::IndexDomain,
    registry::OperationManifest,
};
pub use absolute::AbsoluteOperation;
pub use ceil::CeilOperation;
pub use clip::ClipOperation;
pub use cube_root::CubeRootOperation;
pub use exponential::ExponentialOperation;
pub use floor::FloorOperation;
use graphrecords_core::GraphRecord;
pub use logarithm::LogarithmOperation;
pub use negate::NegateOperation;
pub use round::RoundOperation;
pub use sign::SignOperation;
pub use square_root::SquareRootOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        absolute::operation_manifest(),
        ceil::operation_manifest(),
        clip::operation_manifest(),
        cube_root::operation_manifest(),
        exponential::operation_manifest(),
        floor::operation_manifest(),
        logarithm::operation_manifest(),
        negate::operation_manifest(),
        round::operation_manifest(),
        sign::operation_manifest(),
        square_root::operation_manifest(),
    ]
}

type NumericFunction<'a, V> =
    fn(<V as ValueDomain>::Value<'a>, &'static str) -> QueryResult<<V as ValueDomain>::Value<'a>>;

fn numeric_indexed<'a, I, V>(
    graphrecord: &'a GraphRecord,
    operation: NumericFunction<'a, V>,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, V, Preserving>
where
    I: IndexDomain,
    V: ValueDomain,
{
    Pipeline::keyed(move |address, item: QueryResult<_>| {
        item.and_then(|value| {
            operation(value, label)
                .map_err(|failure| failure.at_address::<I>(graphrecord, &address))
        })
    })
}

fn numeric_bare<'a, V>(
    operation: NumericFunction<'a, V>,
    label: &'static str,
) -> BarePipeline<'a, V, V, Preserving>
where
    V: ValueDomain,
{
    Pipeline::new(move |item: QueryResult<_>| item.and_then(|value| operation(value, label)))
}
