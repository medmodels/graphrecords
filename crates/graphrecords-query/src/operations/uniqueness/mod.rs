mod drop_duplicates;
mod is_duplicated;
mod unique;

use crate::registry::OperationManifest;
pub use drop_duplicates::DropDuplicatesOperation;
pub use is_duplicated::IsDuplicatedOperation;
pub use unique::UniqueOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        drop_duplicates::operation_manifest(),
        is_duplicated::operation_manifest(),
        unique::operation_manifest(),
    ]
}
