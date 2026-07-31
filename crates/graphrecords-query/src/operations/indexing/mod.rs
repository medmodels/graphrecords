mod expanded;
mod index;
mod resolve;
mod select;

use crate::registry::OperationManifest;
pub use expanded::{ChildIndexOperation, ParentIndexOperation};
pub use index::IndexOperation;
pub use resolve::ResolveOperation;
pub use select::SelectOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        index::operation_manifest(),
        resolve::operation_manifest(),
        select::operation_manifest(),
        expanded::parent_index::operation_manifest(),
        expanded::child_index::operation_manifest(),
    ]
}
