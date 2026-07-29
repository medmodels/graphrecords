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
    QueryResult, ValueType,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Preserving},
    index::IndexDomain,
};
pub use absolute::AbsoluteOperation;
pub use ceil::CeilOperation;
pub use clip::ClipOperation;
pub use cube_root::CubeRootOperation;
pub use exponential::ExponentialOperation;
pub use floor::FloorOperation;
pub use logarithm::LogarithmOperation;
pub use negate::NegateOperation;
pub use round::RoundOperation;
pub use sign::SignOperation;
pub use square_root::SquareRootOperation;

type NumericFunction<'a, V> =
    fn(&'static str, <V as ValueType>::Value<'a>) -> QueryResult<<V as ValueType>::Value<'a>>;

fn numeric_indexed<'a, I, V>(
    label: &'static str,
    operation: NumericFunction<'a, V>,
) -> IndexedValuePipeline<'a, I, V, V, Preserving>
where
    I: IndexDomain,
    V: ValueType,
{
    Pipeline::keyed(move |index, item: QueryResult<_>| {
        item.and_then(|value| operation(label, value).map_err(|failure| failure.at::<I>(&index)))
    })
}

fn numeric_bare<'a, V>(
    label: &'static str,
    operation: NumericFunction<'a, V>,
) -> BarePipeline<'a, V, V, Preserving>
where
    V: ValueType,
{
    Pipeline::new(move |item: QueryResult<_>| item.and_then(|value| operation(label, value)))
}
