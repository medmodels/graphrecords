mod edges;
mod endpoint;
mod neighbors;
mod nodes;
mod via_edges;
mod via_neighbors;
mod via_nodes;

use crate::{BoxedIterator, registry::OperationManifest};
pub use edges::EdgesOperation;
pub use endpoint::EndpointOperation;
pub use graphrecords_core::graphrecord::EdgeDirection;
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeAddress, NodeAddress, StateView},
};
pub use neighbors::NeighborsOperation;
pub use nodes::NodesOperation;
pub use via_edges::ViaEdgesOperation;
pub use via_neighbors::ViaNeighborsOperation;
pub use via_nodes::ViaNodesOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        edges::operation_manifest(),
        neighbors::operation_manifest(),
        nodes::operation_manifest(),
        via_edges::operation_manifest(),
        via_neighbors::operation_manifest(),
        via_nodes::operation_manifest(),
        endpoint::via_source_node::operation_manifest(),
        endpoint::via_target_node::operation_manifest(),
    ]
}

pub trait EdgesForNode {
    fn edges_for_node(
        self,
        graphrecord: &GraphRecord,
        node: NodeAddress,
    ) -> BoxedIterator<'_, EdgeAddress>;
}

impl EdgesForNode for EdgeDirection {
    fn edges_for_node(
        self,
        graphrecord: &GraphRecord,
        node: NodeAddress,
    ) -> BoxedIterator<'_, EdgeAddress> {
        let state = StateView::of(graphrecord);

        match self {
            Self::Outgoing => Box::new(state.outgoing_edge_addresses(node)),
            Self::Incoming => Box::new(state.incoming_edge_addresses(node)),
            Self::Both => Box::new(state.incident_edge_addresses(node)),
        }
    }
}

pub trait NeighborsForNode {
    fn neighbors_for_node(
        self,
        graphrecord: &GraphRecord,
        node: NodeAddress,
    ) -> BoxedIterator<'_, NodeAddress>;
}

impl NeighborsForNode for EdgeDirection {
    fn neighbors_for_node(
        self,
        graphrecord: &GraphRecord,
        node: NodeAddress,
    ) -> BoxedIterator<'_, NodeAddress> {
        let state = StateView::of(graphrecord);

        match self {
            Self::Outgoing => Box::new(state.outgoing_neighbor_addresses(node)),
            Self::Incoming => Box::new(state.incoming_neighbor_addresses(node)),
            Self::Both => Box::new(state.neighbor_addresses(node)),
        }
    }
}
