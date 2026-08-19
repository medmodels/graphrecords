#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct EdgeIndex {
    tag: u64,
    offset: u32,
}

impl EdgeIndex {
    pub(crate) const fn new(tag: u64, offset: u32) -> Self {
        Self { tag, offset }
    }

    pub(crate) const fn tag(&self) -> u64 {
        self.tag
    }

    pub(crate) const fn offset(&self) -> u32 {
        self.offset
    }
}

impl Display for EdgeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{:016x}:{}", self.tag, self.offset)
    }
}

#[cfg(test)]
mod test {
    use super::EdgeIndex;
    use std::collections::HashSet;

    #[test]
    fn test_new() {
        let edge_index = EdgeIndex::new(0x0123_4567_89AB_CDEF, 7);

        assert_eq!(0x0123_4567_89AB_CDEF, edge_index.tag());
        assert_eq!(7, edge_index.offset());

        let extremes = EdgeIndex::new(u64::MAX, u32::MAX);

        assert_eq!(u64::MAX, extremes.tag());
        assert_eq!(u32::MAX, extremes.offset());
    }

    #[test]
    fn test_eq() {
        let first = EdgeIndex::new(42, 3);
        let second = first;

        assert_eq!(EdgeIndex::new(42, 3), second);
        assert_ne!(EdgeIndex::new(42, 4), first);
        assert_ne!(EdgeIndex::new(43, 3), first);
    }

    #[test]
    fn test_hash() {
        let edge_indices: HashSet<EdgeIndex> = [
            EdgeIndex::new(1, 0),
            EdgeIndex::new(1, 1),
            EdgeIndex::new(1, 0),
        ]
        .into_iter()
        .collect();

        assert_eq!(2, edge_indices.len());
    }

    #[test]
    fn test_display() {
        assert_eq!("0000000000000001:7", EdgeIndex::new(1, 7).to_string());
        assert_eq!(
            "0123456789abcdef:0",
            EdgeIndex::new(0x0123_4567_89AB_CDEF, 0).to_string()
        );
    }
}
