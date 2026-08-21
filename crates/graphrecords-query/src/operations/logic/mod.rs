mod and;
mod exclusive_or;
mod not;
mod or;

use crate::{
    IndexDomain, Mask,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Retention},
    operations::{ArgumentSource, Keyed, Unaligned},
    registry::OperationManifest,
};
pub use and::AndOperation;
pub use exclusive_or::ExclusiveOrOperation;
use graphrecords_core::GraphRecord;
pub use not::NotOperation;
pub use or::OrOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        and::operation_manifest(),
        exclusive_or::operation_manifest(),
        not::operation_manifest(),
        or::operation_manifest(),
    ]
}

fn combine_masks_indexed<'a, I, M>(
    graphrecord: &'a GraphRecord,
    prepared: M::Prepared<'a>,
    operation: fn(bool, bool) -> bool,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, Mask, Mask, M::Retention>
where
    I: IndexDomain,
    M: ArgumentSource<Keyed<I>, Mask>,
    M::Prepared<'a>: 'a,
{
    Pipeline::keyed(move |address, left| match left {
        Err(failure) => M::Retention::keep(Err(failure)),
        Ok(left) => {
            let step = M::resolve(graphrecord, &prepared, &address, label);

            M::Retention::map_step(step, |resolved| {
                resolved.map(|right| operation(left, right))
            })
        }
    })
}

fn combine_masks_bare<'a, M>(
    graphrecord: &'a GraphRecord,
    prepared: M::Prepared<'a>,
    operation: fn(bool, bool) -> bool,
    label: &'static str,
) -> BarePipeline<'a, Mask, Mask, M::Retention>
where
    M: ArgumentSource<Unaligned, Mask>,
    M::Prepared<'a>: 'a,
{
    Pipeline::new(move |left| match left {
        Err(failure) => M::Retention::keep(Err(failure)),
        Ok(left) => {
            let step = M::resolve(graphrecord, &prepared, &(), label);

            M::Retention::map_step(step, |resolved| {
                resolved.map(|right| operation(left, right))
            })
        }
    })
}

fn combine_masks_kleene_indexed<'a, I, M>(
    graphrecord: &'a GraphRecord,
    prepared: M::Prepared<'a>,
    determining: bool,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, Mask, Mask, M::Retention>
where
    I: IndexDomain,
    M: ArgumentSource<Keyed<I>, Mask>,
    M::Prepared<'a>: 'a,
{
    Pipeline::keyed(move |address, left| {
        let step = M::resolve(graphrecord, &prepared, &address, label);

        match left {
            Err(failure) => match M::Retention::collapse(step) {
                Some(Ok(right)) if right == determining => M::Retention::keep(Ok(determining)),
                _ => M::Retention::keep(Err(failure)),
            },
            Ok(left) => M::Retention::map_step(step, |resolved| {
                if left == determining {
                    Ok(determining)
                } else {
                    resolved
                }
            }),
        }
    })
}

fn combine_masks_kleene_bare<'a, M>(
    graphrecord: &'a GraphRecord,
    prepared: M::Prepared<'a>,
    determining: bool,
    label: &'static str,
) -> BarePipeline<'a, Mask, Mask, M::Retention>
where
    M: ArgumentSource<Unaligned, Mask>,
    M::Prepared<'a>: 'a,
{
    Pipeline::new(move |left| {
        let step = M::resolve(graphrecord, &prepared, &(), label);

        match left {
            Err(failure) => match M::Retention::collapse(step) {
                Some(Ok(right)) if right == determining => M::Retention::keep(Ok(determining)),
                _ => M::Retention::keep(Err(failure)),
            },
            Ok(left) => M::Retention::map_step(step, |resolved| {
                if left == determining {
                    Ok(determining)
                } else {
                    resolved
                }
            }),
        }
    })
}
