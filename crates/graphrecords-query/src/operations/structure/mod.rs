mod attribute;
mod attributes;
mod filter;
mod has_attribute;
mod in_group;

use crate::registry::OperationManifest;
pub use attribute::AttributeOperation;
pub use attributes::AttributesOperation;
pub use filter::FilterOperation;
pub use has_attribute::HasAttributeOperation;
pub use in_group::InGroupOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        attribute::operation_manifest(),
        attributes::operation_manifest(),
        filter::operation_manifest(),
        has_attribute::operation_manifest(),
        in_group::operation_manifest(),
    ]
}
