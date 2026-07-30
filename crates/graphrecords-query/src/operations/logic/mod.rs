mod and;
mod exclusive_or;
mod not;
mod or;

use crate::{
    IndexDomain, Mask,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Retention},
    operations::{ArgumentSource, Keyed, Unaligned},
};
pub use and::AndOperation;
pub use exclusive_or::ExclusiveOrOperation;
pub use not::NotOperation;
pub use or::OrOperation;

fn combine_masks_indexed<'a, I, M>(
    prepared: M::Prepared<'a>,
    label: &'static str,
    operation: fn(bool, bool) -> bool,
) -> IndexedValuePipeline<'a, I, Mask, Mask, M::Retention>
where
    I: IndexDomain,
    M: ArgumentSource<Keyed<I>, Value<'a> = bool>,
    M::Prepared<'a>: 'a,
{
    Pipeline::keyed(move |index, left| match left {
        Err(failure) => M::Retention::keep(Err(failure)),
        Ok(left) => {
            let step = M::resolve(&prepared, &index, label);

            M::Retention::map_step(step, |resolved| {
                resolved.map(|right| operation(left, right))
            })
        }
    })
}

fn combine_masks_bare<'a, M>(
    prepared: M::Prepared<'a>,
    label: &'static str,
    operation: fn(bool, bool) -> bool,
) -> BarePipeline<'a, Mask, Mask, M::Retention>
where
    M: ArgumentSource<Unaligned, Value<'a> = bool>,
    M::Prepared<'a>: 'a,
{
    Pipeline::new(move |left| match left {
        Err(failure) => M::Retention::keep(Err(failure)),
        Ok(left) => {
            let step = M::resolve(&prepared, &(), label);

            M::Retention::map_step(step, |resolved| {
                resolved.map(|right| operation(left, right))
            })
        }
    })
}
