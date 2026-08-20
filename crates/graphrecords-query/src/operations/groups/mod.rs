mod edge_count;
#[allow(clippy::module_inception)]
mod groups;
mod member_edges;
mod node_count;
mod via_groups;
mod via_member_edges;

use crate::registry::OperationManifest;
pub use edge_count::EdgeCountOperation;
pub use groups::GroupsOperation;
pub use member_edges::MemberEdgesOperation;
pub use node_count::NodeCountOperation;
pub use via_groups::ViaGroupsOperation;
pub use via_member_edges::ViaMemberEdgesOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        edge_count::operation_manifest(),
        groups::operation_manifest(),
        member_edges::operation_manifest(),
        node_count::operation_manifest(),
        via_groups::operation_manifest(),
        via_member_edges::operation_manifest(),
    ]
}
