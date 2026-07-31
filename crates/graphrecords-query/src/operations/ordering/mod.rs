mod first;
mod last;
mod reverse_order;
mod shuffle;
mod sort;
mod sort_by;
mod take;
mod unorder;

use crate::registry::OperationManifest;
pub use first::FirstOperation;
pub use last::LastOperation;
pub use reverse_order::ReverseOrderOperation;
pub use shuffle::ShuffleOperation;
pub use sort::SortOperation;
pub use sort_by::SortByOperation;
pub use take::TakeOperation;
pub use unorder::UnorderOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        first::operation_manifest(),
        last::operation_manifest(),
        reverse_order::operation_manifest(),
        shuffle::operation_manifest(),
        sort::operation_manifest(),
        sort_by::operation_manifest(),
        take::operation_manifest(),
        unorder::operation_manifest(),
    ]
}
