mod cast;
mod discard_index;
mod discard_value;
mod enumerate;
mod expand_to;
mod transition;

use crate::registry::OperationManifest;
pub use cast::CastOperation;
pub use discard_index::DiscardIndexOperation;
pub use discard_value::DiscardValueOperation;
pub use enumerate::EnumerateOperation;
pub use expand_to::ExpandToOperation;
pub use transition::TransitionOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        cast::bool::operation_manifest(),
        cast::date_time::operation_manifest(),
        cast::duration::operation_manifest(),
        cast::float::operation_manifest(),
        cast::int::operation_manifest(),
        cast::string::operation_manifest(),
        discard_index::operation_manifest(),
        discard_value::operation_manifest(),
        enumerate::operation_manifest(),
        expand_to::operation_manifest(),
    ]
}
