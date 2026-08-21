mod address;
mod adjacency_chunk;
mod attribute_chunk;
mod attribute_directory;
mod chunk_tree;
mod dictionary;
mod edge_epoch;
mod endpoint_chunk;
mod graph_state;
mod group_directory;
mod key_chunk;
mod membership_chunk;
mod presence;

#[cfg(all(feature = "io", test))]
pub use self::endpoint_chunk::EdgeEndpoints;
pub use self::{
    address::{AttributeAddress, EdgeAddress, GroupAddress, NodeAddress},
    edge_epoch::EdgeEpoch,
    graph_state::GraphState,
};
use std::sync::atomic::{AtomicU64, Ordering};

pub const CHUNK_LOCAL_ADDRESS_BITS: u32 = 8;
pub const ADDRESSES_PER_CHUNK: usize = 1 << CHUNK_LOCAL_ADDRESS_BITS;
pub const CHUNK_BITMAP_WORDS: usize = ADDRESSES_PER_CHUNK / 64;
pub const CHUNK_TREE_BITS_PER_LEVEL: u32 = 6;
pub const CHUNK_TREE_CHILDREN_PER_BRANCH: usize = 1 << CHUNK_TREE_BITS_PER_LEVEL;

static NEXT_STATE_IDENTITY: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StateIdentity(u64);

impl StateIdentity {
    pub(crate) fn mint() -> Self {
        Self(NEXT_STATE_IDENTITY.fetch_add(1, Ordering::Relaxed))
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod test {
    use super::StateIdentity;

    #[test]
    fn test_mint() {
        let first = StateIdentity::mint();
        let second = StateIdentity::mint();

        assert_ne!(first, second);
    }

    #[test]
    fn test_value() {
        let first = StateIdentity::mint();
        let second = StateIdentity::mint();

        assert_ne!(first.value(), second.value());
    }
}
