mod edges;
mod endpoint;
mod neighbors;
mod nodes;
mod via_edges;
mod via_neighbors;
mod via_nodes;

pub use edges::EdgesOperation;
pub use endpoint::EndpointOperation;
pub use neighbors::NeighborsOperation;
pub use nodes::NodesOperation;
use std::fmt::{self, Display, Formatter};
pub use via_edges::ViaEdgesOperation;
pub use via_neighbors::ViaNeighborsOperation;
pub use via_nodes::ViaNodesOperation;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EdgeDirection {
    Incoming,
    Outgoing,
    Both,
}

impl Display for EdgeDirection {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incoming => formatter.write_str("incoming"),
            Self::Outgoing => formatter.write_str("outgoing"),
            Self::Both => formatter.write_str("both"),
        }
    }
}
