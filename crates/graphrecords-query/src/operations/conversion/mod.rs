mod cast;
mod discard_index;
mod discard_value;
mod enumerate;
mod inherit;
mod transition;

use crate::registry::OperationManifest;
pub use cast::CastOperation;
pub use discard_index::DiscardIndexOperation;
pub use discard_value::DiscardValueOperation;
pub use enumerate::EnumerateOperation;
pub use inherit::InheritOperation;
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
        inherit::operation_manifest(),
        transition::attribute_name::operation_manifest(),
        transition::attribute_name_index::operation_manifest(),
        transition::bool_index::operation_manifest(),
        transition::failure_kind_index::operation_manifest(),
        transition::failure_kind_value::operation_manifest(),
        transition::group_index::operation_manifest(),
        transition::mask::operation_manifest(),
        transition::node_index::operation_manifest(),
        transition::positional_index::operation_manifest(),
        transition::scalar::operation_manifest(),
        transition::value_index::operation_manifest(),
    ]
}
