use super::EdgeAddress;
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::hash::{BuildHasher, RandomState};

#[derive(Debug, Clone, Copy)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct EdgeEpoch {
    tag: u64,
    first_address: EdgeAddress,
    edge_count: u32,
}

impl EdgeEpoch {
    pub(crate) fn mint(existing: &[Self], first_address: EdgeAddress, edge_count: u32) -> Self {
        let mut tag = Self::random_tag();

        while existing.iter().any(|epoch| epoch.tag == tag) {
            tag = Self::random_tag();
        }

        Self {
            tag,
            first_address,
            edge_count,
        }
    }

    fn random_tag() -> u64 {
        RandomState::new().hash_one(0_u64)
    }

    pub(crate) const fn tag(&self) -> u64 {
        self.tag
    }

    pub(crate) const fn first_address(&self) -> EdgeAddress {
        self.first_address
    }

    pub(crate) const fn edge_count(&self) -> u32 {
        self.edge_count
    }
}

#[cfg(test)]
mod test {
    use super::EdgeEpoch;
    use crate::graphrecord::state::EdgeAddress;

    fn create_epoch() -> EdgeEpoch {
        EdgeEpoch::mint(&[], EdgeAddress::new(7), 4)
    }

    #[test]
    fn test_mint() {
        let epoch = EdgeEpoch::mint(&[], EdgeAddress::new(5), 3);

        assert_eq!(5, epoch.first_address().index());
        assert_eq!(3, epoch.edge_count());

        let first = EdgeEpoch::mint(&[], EdgeAddress::new(0), 1);
        let second = EdgeEpoch::mint(&[first], EdgeAddress::new(1), 1);

        assert_ne!(first.tag(), second.tag());
    }

    #[test]
    fn test_tag() {
        let epoch = create_epoch();

        assert_eq!(epoch.tag(), epoch.tag());
    }

    #[test]
    fn test_first_address() {
        assert_eq!(7, create_epoch().first_address().index());
    }

    #[test]
    fn test_edge_count() {
        assert_eq!(4, create_epoch().edge_count());
    }
}
