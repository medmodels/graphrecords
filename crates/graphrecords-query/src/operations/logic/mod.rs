mod and;
mod is_max;
mod is_min;
mod not;
mod or;
mod xor;

use crate::{
    IndexDomain, QueryResult,
    operations::{ArgumentSource, Keyed, Pipeline, Retention, Unaligned},
};
pub use and::AndOperation;
pub use not::NotOperation;
pub use or::OrOperation;
pub use xor::XorOperation;

type MaskElement<'a, I> = (<I as IndexDomain>::Index<'a>, QueryResult<bool>);

fn combine_bare_masks<'a, M>(
    prepared: M::Prepared<'a>,
    label: &'static str,
    operation: fn(bool, bool) -> bool,
) -> Pipeline<'a, QueryResult<bool>, QueryResult<bool>, M::Retention>
where
    M: ArgumentSource<Unaligned, Value<'a> = bool>,
    M::Prepared<'a>: 'a,
{
    Pipeline::element_wise(move |left| match left {
        Err(failure) => <M::Retention as Retention>::keep(Err(failure)),
        Ok(left) => {
            let step = M::resolve(&prepared, &(), label);

            <M::Retention as Retention>::map_step(step, |resolved| {
                resolved.map(|right| operation(left, right))
            })
        }
    })
}

fn combine_masks<'a, I, M>(
    prepared: M::Prepared<'a>,
    label: &'static str,
    operation: fn(bool, bool) -> bool,
) -> Pipeline<'a, MaskElement<'a, I>, MaskElement<'a, I>, M::Retention>
where
    I: IndexDomain,
    M: ArgumentSource<Keyed<I>, Value<'a> = bool>,
    M::Prepared<'a>: 'a,
{
    Pipeline::element_wise(move |(index, left)| match left {
        Err(failure) => <M::Retention as Retention>::keep((index, Err(failure))),
        Ok(left) => {
            let step = M::resolve(&prepared, &index, label);

            <M::Retention as Retention>::map_step(step, |resolved| {
                (index, resolved.map(|right| operation(left, right)))
            })
        }
    })
}
