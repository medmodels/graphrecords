use super::{NodeAddress, presence::PresenceBitmap};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct EdgeEndpoints {
    pub source_address: NodeAddress,
    pub target_address: NodeAddress,
}

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct EndpointChunk {
    present: PresenceBitmap,
    cells: Vec<EdgeEndpoints>,
}

impl EndpointChunk {
    pub const fn new() -> Self {
        Self {
            present: PresenceBitmap::new(),
            cells: Vec::new(),
        }
    }

    pub fn get(&self, cell_index: usize) -> Option<&EdgeEndpoints> {
        if !self.present.contains(cell_index) {
            return None;
        }

        Some(&self.cells[self.present.rank(cell_index)])
    }

    pub fn set(&mut self, cell_index: usize, endpoints: EdgeEndpoints) {
        if self.present.contains(cell_index) {
            let rank = self.present.rank(cell_index);
            self.cells[rank] = endpoints;
            return;
        }

        let rank = self.present.set(cell_index);
        self.cells.insert(rank, endpoints);
    }

    pub fn remove(&mut self, cell_index: usize) -> bool {
        if !self.present.contains(cell_index) {
            return false;
        }

        let rank = self.present.clear(cell_index);
        self.cells.remove(rank);

        true
    }

    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, &EdgeEndpoints)> + '_ {
        self.present.iter_present().zip(self.cells.iter())
    }
}

impl Default for EndpointChunk {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::{EdgeEndpoints, EndpointChunk};
    use crate::graphrecord::state::NodeAddress;

    fn create_endpoints(source: u32, target: u32) -> EdgeEndpoints {
        EdgeEndpoints {
            source_address: NodeAddress::new(source),
            target_address: NodeAddress::new(target),
        }
    }

    #[test]
    fn test_new() {
        let chunk = EndpointChunk::new();

        assert!(chunk.is_empty());
        assert_eq!(0, chunk.iter().count());
        assert_eq!(0, EndpointChunk::default().iter().count());
    }

    #[test]
    fn test_get() {
        let chunk = EndpointChunk::new();

        assert!(chunk.get(0).is_none());

        let mut chunk = EndpointChunk::new();

        chunk.set(5, create_endpoints(1, 2));

        assert_eq!(Some(&create_endpoints(1, 2)), chunk.get(5));
    }

    #[test]
    fn test_set() {
        let mut chunk = EndpointChunk::new();

        chunk.set(5, create_endpoints(1, 2));

        assert_eq!(Some(&create_endpoints(1, 2)), chunk.get(5));
        assert_eq!(1, chunk.iter().count());

        chunk.set(5, create_endpoints(3, 4));

        assert_eq!(Some(&create_endpoints(3, 4)), chunk.get(5));
        assert_eq!(1, chunk.iter().count());
    }

    #[test]
    fn test_remove() {
        let mut chunk = EndpointChunk::new();

        chunk.set(5, create_endpoints(1, 2));

        assert!(!chunk.remove(6));
        assert!(chunk.remove(5));

        assert!(chunk.get(5).is_none());
        assert!(chunk.is_empty());
    }

    #[test]
    fn test_is_empty() {
        let mut chunk = EndpointChunk::new();

        assert!(chunk.is_empty());

        chunk.set(5, create_endpoints(1, 2));

        assert!(!chunk.is_empty());

        assert!(chunk.remove(5));

        assert!(chunk.is_empty());
    }

    #[test]
    fn test_iter() {
        let mut chunk = EndpointChunk::new();

        chunk.set(200, create_endpoints(9, 10));
        chunk.set(3, create_endpoints(1, 2));
        chunk.set(64, create_endpoints(5, 6));

        let observed: Vec<_> = chunk
            .iter()
            .map(|(cell_index, value)| (cell_index, *value))
            .collect();

        assert_eq!(
            vec![
                (3, create_endpoints(1, 2)),
                (64, create_endpoints(5, 6)),
                (200, create_endpoints(9, 10)),
            ],
            observed
        );
    }
}
