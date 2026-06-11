mod and;
mod is_max;
mod is_min;
mod not;
mod or;
mod xor;

use crate::{
    BoxedIterator, IndexDomain, QueryResult,
    operations::{ArgumentSource, OnMissing},
};
pub use and::AndOperation;
pub use not::NotOperation;
pub use or::OrOperation;
pub use xor::XorOperation;

type CombinedMasks<'a, I> = BoxedIterator<'a, (<I as IndexDomain>::Index<'a>, QueryResult<bool>)>;

fn combine_masks<'a, I, M>(
    values: CombinedMasks<'a, I>,
    prepared: M::Prepared<'a>,
    label: &'static str,
    operation: fn(bool, bool) -> bool,
) -> CombinedMasks<'a, I>
where
    I: IndexDomain,
    M: ArgumentSource<I, Value = bool>,
    M::Prepared<'a>: 'a,
{
    Box::new(values.filter_map(move |(index, left)| match left {
        Err(failure) => Some((index, Err(failure))),
        Ok(left) => match M::resolve(&prepared, &index, label, OnMissing::Raise) {
            Ok(Some(right)) => Some((index, Ok(operation(left, right)))),
            Ok(None) => None,
            Err(failure) => Some((index, Err(failure))),
        },
    }))
}
