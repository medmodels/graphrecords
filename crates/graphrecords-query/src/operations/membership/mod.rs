mod is_in;

use crate::registry::OperationManifest;
pub use is_in::IsInOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![is_in::operation_manifest()]
}
