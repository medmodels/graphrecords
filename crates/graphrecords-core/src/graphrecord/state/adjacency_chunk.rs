use super::{ADDRESSES_PER_CHUNK, EdgeAddress, NodeAddress};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const ADJACENCY_SPILL_DEGREE: usize = 128;
pub const MAX_ENTRIES_PER_SPILL_CHUNK: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct AdjacencyEntry {
    pub neighbor_address: NodeAddress,
    pub edge_address: EdgeAddress,
}

impl AdjacencyEntry {
    const fn sort_key(self) -> (u32, u32) {
        (self.neighbor_address.index(), self.edge_address.index())
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct AdjacencyChunk {
    #[cfg_attr(any(feature = "serde", feature = "io"), serde(with = "offsets_serde"))]
    offsets: [u16; ADDRESSES_PER_CHUNK + 1],
    entries: Vec<AdjacencyEntry>,
    spills: Vec<SpillEntry>,
}

impl AdjacencyChunk {
    pub const fn new() -> Self {
        Self {
            offsets: [0; ADDRESSES_PER_CHUNK + 1],
            entries: Vec::new(),
            spills: Vec::new(),
        }
    }

    pub fn entries(&self, cell_index: usize) -> impl Iterator<Item = &AdjacencyEntry> + '_ {
        if let Some(spill_position) = self.find_spill(cell_index) {
            return AdjacencyEntryIterator::Spilled(SpillChunksIterator::new(
                &self.spills[spill_position].chunks,
            ));
        }

        let start = self.offsets[cell_index] as usize;
        let end = self.offsets[cell_index + 1] as usize;

        AdjacencyEntryIterator::Inline(self.entries[start..end].iter())
    }

    pub fn add(&mut self, cell_index: usize, entry: AdjacencyEntry) -> bool {
        if let Some(spill_position) = self.find_spill(cell_index) {
            return Self::insert_into_spill_chunks(&mut self.spills[spill_position].chunks, entry);
        }

        let start = self.offsets[cell_index] as usize;
        let end = self.offsets[cell_index + 1] as usize;

        let target_key = entry.sort_key();
        let relative_offset =
            self.entries[start..end].partition_point(|candidate| candidate.sort_key() < target_key);
        let insertion_position = start + relative_offset;

        if relative_offset < end - start
            && self.entries[insertion_position].sort_key() == target_key
        {
            return false;
        }

        if end - start >= ADJACENCY_SPILL_DEGREE {
            self.spill_run(cell_index, entry);
            return true;
        }

        self.entries.insert(insertion_position, entry);

        for offset in &mut self.offsets[cell_index + 1..] {
            *offset += 1;
        }

        true
    }

    pub fn remove(&mut self, cell_index: usize, edge_address: EdgeAddress) -> bool {
        if let Some(spill_position) = self.find_spill(cell_index) {
            let removed = Self::remove_from_spill_chunks(
                &mut self.spills[spill_position].chunks,
                edge_address,
            );

            if removed && self.spills[spill_position].chunks.is_empty() {
                self.spills.remove(spill_position);
            }

            return removed;
        }

        let start = self.offsets[cell_index] as usize;
        let end = self.offsets[cell_index + 1] as usize;

        let Some(relative_offset) = self.entries[start..end]
            .iter()
            .position(|candidate| candidate.edge_address == edge_address)
        else {
            return false;
        };

        self.entries.remove(start + relative_offset);

        for offset in &mut self.offsets[cell_index + 1..] {
            *offset -= 1;
        }

        true
    }

    pub fn remove_cell(&mut self, cell_index: usize) -> usize {
        if let Some(spill_position) = self.find_spill(cell_index) {
            let removed = self.spills.remove(spill_position);

            return Self::spill_chunks_len(&removed.chunks);
        }

        let start = self.offsets[cell_index] as usize;
        let end = self.offsets[cell_index + 1] as usize;
        let removed_count = end - start;

        if removed_count == 0 {
            return 0;
        }

        self.entries.drain(start..end);

        for offset in &mut self.offsets[cell_index + 1..] {
            *offset -= removed_count as u16;
        }

        removed_count
    }

    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.spills.is_empty()
    }

    fn find_spill(&self, cell_index: usize) -> Option<usize> {
        let cell_index = cell_index as u8;
        self.spills
            .binary_search_by(|candidate| candidate.cell_index.cmp(&cell_index))
            .ok()
    }

    fn spill_chunks_len(chunks: &[Arc<SpillChunk>]) -> usize {
        chunks.iter().map(|chunk| chunk.entries.len()).sum()
    }

    fn spill_run(&mut self, cell_index: usize, entry: AdjacencyEntry) {
        let start = self.offsets[cell_index] as usize;
        let end = self.offsets[cell_index + 1] as usize;
        let run_length = end - start;

        let mut moved_entries: Vec<_> = self
            .entries
            .splice(start..end, std::iter::empty())
            .collect();

        for offset in &mut self.offsets[cell_index + 1..] {
            *offset -= run_length as u16;
        }

        let target_key = entry.sort_key();
        let insertion_position =
            moved_entries.partition_point(|candidate| candidate.sort_key() < target_key);
        moved_entries.insert(insertion_position, entry);

        let spill_chunks = moved_entries
            .chunks(MAX_ENTRIES_PER_SPILL_CHUNK)
            .map(|chunk_entries| {
                Arc::new(SpillChunk {
                    entries: chunk_entries.to_vec(),
                })
            })
            .collect();

        let insertion_position = self
            .spills
            .partition_point(|candidate| candidate.cell_index < cell_index as u8);
        self.spills.insert(
            insertion_position,
            SpillEntry {
                cell_index: cell_index as u8,
                chunks: spill_chunks,
            },
        );
    }

    fn insert_into_spill_chunks(chunks: &mut Vec<Arc<SpillChunk>>, entry: AdjacencyEntry) -> bool {
        if chunks.is_empty() {
            chunks.push(Arc::new(SpillChunk {
                entries: vec![entry],
            }));
            return true;
        }

        let target_key = entry.sort_key();
        let mut chunk_index = chunks.partition_point(|chunk| {
            chunk
                .entries
                .last()
                .is_none_or(|last| last.sort_key() < target_key)
        });

        if chunk_index == chunks.len() {
            chunk_index -= 1;
        }

        let insertion_position = chunks[chunk_index]
            .entries
            .partition_point(|candidate| candidate.sort_key() < target_key);

        if chunks[chunk_index]
            .entries
            .get(insertion_position)
            .map(|candidate| candidate.sort_key())
            == Some(target_key)
        {
            return false;
        }

        let chunk = Arc::make_mut(&mut chunks[chunk_index]);
        chunk.entries.insert(insertion_position, entry);

        if chunk.entries.len() > MAX_ENTRIES_PER_SPILL_CHUNK {
            let split_point = chunk.entries.len() / 2;
            let second_half = chunk.entries.split_off(split_point);

            chunks.insert(
                chunk_index + 1,
                Arc::new(SpillChunk {
                    entries: second_half,
                }),
            );
        }

        true
    }

    fn remove_from_spill_chunks(
        chunks: &mut Vec<Arc<SpillChunk>>,
        edge_address: EdgeAddress,
    ) -> bool {
        let Some((chunk_index, position)) =
            chunks.iter().enumerate().find_map(|(chunk_index, chunk)| {
                chunk
                    .entries
                    .iter()
                    .position(|candidate| candidate.edge_address == edge_address)
                    .map(|position| (chunk_index, position))
            })
        else {
            return false;
        };

        let chunk = Arc::make_mut(&mut chunks[chunk_index]);
        chunk.entries.remove(position);

        if chunk.entries.is_empty() {
            chunks.remove(chunk_index);
        }

        true
    }
}

impl Default for AdjacencyChunk {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(feature = "serde", feature = "io"))]
mod offsets_serde {
    use super::ADDRESSES_PER_CHUNK;
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

    pub(super) fn serialize<S: Serializer>(
        offsets: &[u16; ADDRESSES_PER_CHUNK + 1],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        offsets.as_slice().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<[u16; ADDRESSES_PER_CHUNK + 1], D::Error> {
        let offsets = Vec::<u16>::deserialize(deserializer)?;
        let length = offsets.len();

        offsets
            .try_into()
            .map_err(|_| D::Error::invalid_length(length, &"an offsets array of chunk width"))
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
struct SpillEntry {
    cell_index: u8,
    chunks: Vec<Arc<SpillChunk>>,
}

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
struct SpillChunk {
    entries: Vec<AdjacencyEntry>,
}

enum AdjacencyEntryIterator<'a> {
    Inline(std::slice::Iter<'a, AdjacencyEntry>),
    Spilled(SpillChunksIterator<'a>),
}

impl<'a> Iterator for AdjacencyEntryIterator<'a> {
    type Item = &'a AdjacencyEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(iterator) => iterator.next(),
            Self::Spilled(iterator) => iterator.next(),
        }
    }
}

struct SpillChunksIterator<'a> {
    chunks: std::slice::Iter<'a, Arc<SpillChunk>>,
    current: std::slice::Iter<'a, AdjacencyEntry>,
}

impl<'a> SpillChunksIterator<'a> {
    fn new(spill_chunks: &'a [Arc<SpillChunk>]) -> Self {
        let mut chunks = spill_chunks.iter();
        let current = chunks
            .next()
            .map_or_else(Default::default, |chunk| chunk.entries.iter());

        Self { chunks, current }
    }
}

impl<'a> Iterator for SpillChunksIterator<'a> {
    type Item = &'a AdjacencyEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.current.next() {
                return Some(entry);
            }

            self.current = self.chunks.next()?.entries.iter();
        }
    }
}

#[cfg(test)]
mod test {
    use super::{
        ADJACENCY_SPILL_DEGREE, AdjacencyChunk, AdjacencyEntry, MAX_ENTRIES_PER_SPILL_CHUNK,
    };
    use crate::graphrecord::state::{EdgeAddress, NodeAddress};

    fn create_entry(neighbor: u32, edge: u32) -> AdjacencyEntry {
        AdjacencyEntry {
            neighbor_address: NodeAddress::new(neighbor),
            edge_address: EdgeAddress::new(edge),
        }
    }

    #[test]
    fn test_new() {
        let chunk = AdjacencyChunk::new();

        assert!(chunk.is_empty());
        assert_eq!(0, chunk.entries(0).count());
        assert_eq!(0, AdjacencyChunk::default().entries(0).count());
    }

    #[test]
    fn test_entries() {
        let mut chunk = AdjacencyChunk::new();

        chunk.add(3, create_entry(5, 50));
        chunk.add(3, create_entry(1, 10));

        let observed: Vec<_> = chunk.entries(3).copied().collect();

        assert_eq!(vec![create_entry(1, 10), create_entry(5, 50)], observed);

        let mut chunk = AdjacencyChunk::new();

        for edge_address in 0..=(ADJACENCY_SPILL_DEGREE as u32) {
            chunk.add(5, create_entry(1, edge_address));
        }

        let observed: Vec<_> = chunk
            .entries(5)
            .map(|candidate| candidate.edge_address.index())
            .collect();
        let expected: Vec<_> = (0..=(ADJACENCY_SPILL_DEGREE as u32)).collect();

        assert_eq!(expected, observed);
    }

    #[test]
    fn test_add() {
        let mut chunk = AdjacencyChunk::new();

        chunk.add(3, create_entry(5, 50));
        chunk.add(3, create_entry(1, 10));
        chunk.add(3, create_entry(5, 20));

        let observed: Vec<_> = chunk.entries(3).copied().collect();

        assert_eq!(
            vec![
                create_entry(1, 10),
                create_entry(5, 20),
                create_entry(5, 50)
            ],
            observed
        );
        assert_eq!(3, chunk.entries(3).count());

        let mut chunk = AdjacencyChunk::new();

        assert!(chunk.add(3, create_entry(1, 10)));
        assert!(!chunk.add(3, create_entry(1, 10)));

        assert_eq!(1, chunk.entries(3).count());

        let mut chunk = AdjacencyChunk::new();

        chunk.add(3, create_entry(1, 10));
        chunk.add(7, create_entry(2, 20));

        let run_three: Vec<_> = chunk.entries(3).copied().collect();
        let run_seven: Vec<_> = chunk.entries(7).copied().collect();

        assert_eq!(vec![create_entry(1, 10)], run_three);
        assert_eq!(vec![create_entry(2, 20)], run_seven);

        let mut chunk = AdjacencyChunk::new();

        for edge_address in 0..ADJACENCY_SPILL_DEGREE as u32 {
            chunk.add(5, create_entry(1, edge_address));
        }

        assert_eq!(ADJACENCY_SPILL_DEGREE, chunk.entries(5).count());

        let before: Vec<_> = chunk.entries(5).copied().collect();

        assert!(chunk.add(5, create_entry(1, ADJACENCY_SPILL_DEGREE as u32)));

        assert_eq!(ADJACENCY_SPILL_DEGREE + 1, chunk.entries(5).count());

        let after: Vec<_> = chunk.entries(5).copied().collect();

        assert_eq!(before[..], after[..ADJACENCY_SPILL_DEGREE]);
        assert_eq!(
            create_entry(1, ADJACENCY_SPILL_DEGREE as u32),
            after[ADJACENCY_SPILL_DEGREE]
        );

        assert!(chunk.find_spill(5).is_some());

        let start = chunk.offsets[5];
        let end = chunk.offsets[6];
        assert_eq!(start, end, "Spilled cell must have an empty in-chunk run.");

        let mut chunk = AdjacencyChunk::new();

        let total_entries = ADJACENCY_SPILL_DEGREE + MAX_ENTRIES_PER_SPILL_CHUNK + 10;

        for edge_address in 0..total_entries as u32 {
            chunk.add(5, create_entry(1, edge_address));
        }

        assert_eq!(total_entries, chunk.entries(5).count());

        let spill_position = chunk.find_spill(5).expect("Cell must be spilled.");

        assert!(chunk.spills[spill_position].chunks.len() > 1);

        let observed: Vec<_> = chunk
            .entries(5)
            .map(|candidate| candidate.edge_address.index())
            .collect();
        let expected: Vec<_> = (0..total_entries as u32).collect();

        assert_eq!(expected, observed);

        let mut chunk = AdjacencyChunk::new();

        for edge_address in (0..ADJACENCY_SPILL_DEGREE as u32).rev() {
            chunk.add(5, create_entry(1, edge_address * 2));
        }

        assert!(chunk.find_spill(5).is_none());

        chunk.add(5, create_entry(1, 9999));

        assert!(chunk.find_spill(5).is_some());

        chunk.add(5, create_entry(1, 1));

        let observed: Vec<_> = chunk
            .entries(5)
            .map(|candidate| candidate.edge_address.index())
            .collect();
        let mut expected = observed.clone();
        expected.sort_unstable();

        assert_eq!(expected, observed);

        let mut chunk = AdjacencyChunk::new();

        for edge_address in 0..=(ADJACENCY_SPILL_DEGREE as u32) {
            chunk.add(5, create_entry(1, edge_address));
        }

        let before = chunk.entries(5).count();

        assert!(!chunk.add(5, create_entry(1, 0)));

        assert_eq!(before, chunk.entries(5).count());

        let mut chunk = AdjacencyChunk::new();

        for edge_address in 0..=(ADJACENCY_SPILL_DEGREE as u32) {
            chunk.add(5, create_entry(1, edge_address));
        }

        let mut cloned = chunk.clone();
        assert!(!cloned.add(5, create_entry(1, 0)));

        let original_pointer = std::ptr::from_ref(chunk.entries(5).next().unwrap());
        let cloned_pointer = std::ptr::from_ref(cloned.entries(5).next().unwrap());

        assert_eq!(
            original_pointer, cloned_pointer,
            "Duplicate add must not copy the shared chunk."
        );
    }

    #[test]
    fn test_remove() {
        let mut chunk = AdjacencyChunk::new();

        chunk.add(3, create_entry(1, 10));
        chunk.add(3, create_entry(2, 20));

        assert!(chunk.remove(3, EdgeAddress::new(10)));

        assert_eq!(1, chunk.entries(3).count());
        assert_eq!(
            vec![create_entry(2, 20)],
            chunk.entries(3).copied().collect::<Vec<_>>()
        );

        let mut chunk = AdjacencyChunk::new();

        for edge_address in 0..=(ADJACENCY_SPILL_DEGREE as u32) {
            chunk.add(5, create_entry(1, edge_address));
        }

        assert!(chunk.find_spill(5).is_some());

        for edge_address in 1..=(ADJACENCY_SPILL_DEGREE as u32) {
            chunk.remove(5, EdgeAddress::new(edge_address));
        }

        assert_eq!(1, chunk.entries(5).count());
        assert!(
            chunk.find_spill(5).is_some(),
            "Shrinking must keep the cell spilled."
        );

        let mut chunk = AdjacencyChunk::new();

        for edge_address in 0..=(ADJACENCY_SPILL_DEGREE as u32) {
            chunk.add(5, create_entry(1, edge_address));
        }

        assert!(chunk.find_spill(5).is_some());

        for edge_address in 0..=(ADJACENCY_SPILL_DEGREE as u32) {
            chunk.remove(5, EdgeAddress::new(edge_address));
        }

        assert_eq!(0, chunk.entries(5).count());
        assert!(chunk.find_spill(5).is_none());

        chunk.add(5, create_entry(1, 12345));

        assert_eq!(1, chunk.entries(5).count());
        assert!(chunk.find_spill(5).is_none());
    }

    #[test]
    fn test_invalid_remove() {
        let mut chunk = AdjacencyChunk::new();

        chunk.add(3, create_entry(1, 10));

        assert!(!chunk.remove(3, EdgeAddress::new(999)));
        assert!(!chunk.remove(7, EdgeAddress::new(10)));
    }

    #[test]
    fn test_remove_cell() {
        let mut chunk = AdjacencyChunk::new();

        chunk.add(3, create_entry(1, 10));
        chunk.add(3, create_entry(2, 20));
        chunk.add(7, create_entry(9, 90));

        assert_eq!(2, chunk.remove_cell(3));

        assert_eq!(0, chunk.entries(3).count());
        assert_eq!(
            vec![create_entry(9, 90)],
            chunk.entries(7).copied().collect::<Vec<_>>()
        );
        assert_eq!(0, chunk.remove_cell(3));

        let mut chunk = AdjacencyChunk::new();

        for edge_address in 0..(ADJACENCY_SPILL_DEGREE as u32 + 5) {
            chunk.add(5, create_entry(1, edge_address));
        }

        chunk.add(7, create_entry(2, 999));

        let removed = chunk.remove_cell(5);

        assert_eq!(ADJACENCY_SPILL_DEGREE + 5, removed);
        assert!(chunk.find_spill(5).is_none());
        assert_eq!(0, chunk.entries(5).count());
        assert_eq!(
            vec![create_entry(2, 999)],
            chunk.entries(7).copied().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_is_empty() {
        let mut chunk = AdjacencyChunk::new();

        assert!(chunk.is_empty());

        chunk.add(3, create_entry(1, 10));

        assert!(!chunk.is_empty());

        assert_eq!(1, chunk.remove_cell(3));

        assert!(chunk.is_empty());
    }
}
