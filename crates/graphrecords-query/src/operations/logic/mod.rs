mod and;
mod is_max;
mod is_min;
mod not;
mod or;
mod xor;

use crate::{
    IndexDomain, QueryResult,
    operations::{ArgumentSource, Keyed, OnMissing, Pipeline},
};
pub use and::AndOperation;
pub use not::NotOperation;
pub use or::OrOperation;
pub use xor::XorOperation;

type MaskElement<'a, I> = (<I as IndexDomain>::Index<'a>, QueryResult<bool>);

fn combine_masks<'a, I, M>(
    prepared: M::Prepared<'a>,
    label: &'static str,
    operation: fn(bool, bool) -> bool,
) -> Pipeline<'a, MaskElement<'a, I>, MaskElement<'a, I>>
where
    I: IndexDomain,
    M: ArgumentSource<Keyed<I>, Value = bool>,
    M::Prepared<'a>: 'a,
{
    Pipeline::default().filter_map(move |(index, left)| match left {
        Err(failure) => Some((index, Err(failure))),
        Ok(left) => match M::resolve(&prepared, &index, label, OnMissing::Raise) {
            Ok(Some(right)) => Some((index, Ok(operation(left, right)))),
            Ok(None) => None,
            Err(failure) => Some((index, Err(failure))),
        },
    })
}
