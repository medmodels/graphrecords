use super::{ADDRESSES_PER_CHUNK, CHUNK_LOCAL_ADDRESS_BITS};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};

macro_rules! implement_address {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
        pub struct $name(u32);

        impl $name {
            pub(crate) const fn new(index: u32) -> Self {
                Self(index)
            }

            #[must_use]
            pub const fn index(&self) -> u32 {
                self.0
            }
        }
    };
}

macro_rules! implement_chunked_address {
    ($name:ident) => {
        implement_address!($name);

        impl $name {
            pub(crate) const fn chunk_index(self) -> u32 {
                self.0 >> CHUNK_LOCAL_ADDRESS_BITS
            }

            pub(crate) const fn chunk_local_address(self) -> usize {
                (self.0 as usize) & (ADDRESSES_PER_CHUNK - 1)
            }

            pub(crate) const fn from_chunk_parts(
                chunk_index: u32,
                chunk_local_address: usize,
            ) -> Self {
                Self((chunk_index << CHUNK_LOCAL_ADDRESS_BITS) + chunk_local_address as u32)
            }
        }
    };
}

implement_address!(AttributeAddress);
implement_chunked_address!(EdgeAddress);
implement_chunked_address!(GroupAddress);
implement_chunked_address!(NodeAddress);

#[cfg(test)]
mod test {
    use super::{AttributeAddress, EdgeAddress, GroupAddress, NodeAddress};
    use std::collections::HashSet;

    #[test]
    fn test_new() {
        let first = NodeAddress::new(42);
        let second = first;

        assert_eq!(first, second);
        assert_eq!(NodeAddress::new(42), first);
        assert_ne!(first, NodeAddress::new(43));

        let addresses: HashSet<NodeAddress> = [
            NodeAddress::new(1),
            NodeAddress::new(2),
            NodeAddress::new(1),
        ]
        .into_iter()
        .collect();

        assert_eq!(2, addresses.len());
    }

    #[test]
    fn test_index() {
        assert_eq!(7, NodeAddress::new(7).index());
        assert_eq!(11, EdgeAddress::new(11).index());
        assert_eq!(3, GroupAddress::new(3).index());
        assert_eq!(0, AttributeAddress::new(0).index());
    }

    #[test]
    fn test_chunk_index() {
        assert_eq!(1, NodeAddress::new(257).chunk_index());
        assert_eq!(1, NodeAddress::new(257).chunk_local_address());

        assert_eq!(2, EdgeAddress::new(513).chunk_index());
        assert_eq!(1, EdgeAddress::new(513).chunk_local_address());

        assert_eq!(0, GroupAddress::new(255).chunk_index());
        assert_eq!(255, GroupAddress::new(255).chunk_local_address());
    }

    #[test]
    fn test_from_chunk_parts() {
        assert_eq!(NodeAddress::new(257), NodeAddress::from_chunk_parts(1, 1));
        assert_eq!(EdgeAddress::new(513), EdgeAddress::from_chunk_parts(2, 1));
        assert_eq!(
            GroupAddress::new(255),
            GroupAddress::from_chunk_parts(0, 255)
        );
    }
}
