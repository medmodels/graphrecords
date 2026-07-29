mod absolute;
mod ceil;
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
pub use cube_root::CubeRootOperation;
pub use exponential::ExponentialOperation;
pub use floor::FloorOperation;
pub use logarithm::LogarithmOperation;
pub use negate::NegateOperation;
pub use round::RoundOperation;
pub use sign::SignOperation;
pub use square_root::SquareRootOperation;

type NumericFunction<V> =
    fn(&'static str, <V as ValueType>::Owned) -> QueryResult<<V as ValueType>::Owned>;

fn numeric_indexed<'a, I, V>(
    label: &'static str,
    operation: NumericFunction<V>,
) -> IndexedValuePipeline<'a, I, V, V, Preserving>
where
    I: IndexDomain,
    V: ValueType<Value<'a> = <V as ValueType>::Owned>,
{
    Pipeline::keyed(move |index, item: QueryResult<_>| {
        item.and_then(|value| operation(label, value).map_err(|failure| failure.at::<I>(&index)))
    })
}

fn numeric_bare<'a, V>(
    label: &'static str,
    operation: NumericFunction<V>,
) -> BarePipeline<'a, V, V, Preserving>
where
    V: ValueType<Value<'a> = <V as ValueType>::Owned>,
{
    Pipeline::new(move |item: QueryResult<_>| item.and_then(|value| operation(label, value)))
}
