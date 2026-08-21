use super::{
    ADDRESSES_PER_CHUNK, CHUNK_LOCAL_ADDRESS_BITS, GroupAddress, chunk_tree::ChunkTree,
    dictionary::KeyDictionary, presence::PresenceBitmap,
};
use crate::graphrecord::datatypes::{GroupIndex, GroupIndexView, IdentifierView};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct GroupDirectory {
    #[cfg_attr(any(feature = "serde", feature = "io"), serde(skip))]
    indices: KeyDictionary,
    records: ChunkTree<GroupChunk>,
    next_group_address: GroupAddress,
    group_count: usize,
    ungrouped_node_count: usize,
    ungrouped_edge_count: usize,
}

impl GroupDirectory {
    pub fn new() -> Self {
        Self {
            indices: KeyDictionary::new(),
            records: ChunkTree::new(),
            next_group_address: GroupAddress::new(0),
            group_count: 0,
            ungrouped_node_count: 0,
            ungrouped_edge_count: 0,
        }
    }

    pub fn resolve<'a>(&self, group_index: impl Into<GroupIndexView<'a>>) -> Option<GroupAddress> {
        let group_index = group_index.into();
        let hash = self.indices.hash_one(group_index.identifier_view());

        self.indices.candidates(hash).find_map(|candidate_index| {
            let group_address = GroupAddress::new(candidate_index);
            let record = self.record(group_address)?;

            (IdentifierView::from(record.group_index.identifier())
                == *group_index.identifier_view())
            .then_some(group_address)
        })
    }

    pub fn record(&self, group_address: GroupAddress) -> Option<&Arc<GroupRecord>> {
        self.records
            .get(group_address.chunk_index())?
            .get(group_address.chunk_local_address())
    }

    pub fn record_mut(&mut self, group_address: GroupAddress) -> Option<&mut GroupRecord> {
        self.records
            .get_mut(group_address.chunk_index())?
            .get_mut(group_address.chunk_local_address())
    }

    pub fn group_index(&self, group_address: GroupAddress) -> Option<&GroupIndex> {
        self.record(group_address).map(|record| &record.group_index)
    }

    pub fn add(&mut self, group_index: GroupIndex) -> Option<GroupAddress> {
        if self.resolve(&group_index).is_some() {
            return None;
        }

        let group_address = self.next_group_address;
        self.next_group_address = GroupAddress::new(group_address.index() + 1);

        let hash = self.hash_group_index(&group_index);
        self.indices.insert(hash, group_address.index());

        self.records
            .get_mut_or_default(group_address.chunk_index())
            .set(
                group_address.chunk_local_address(),
                GroupRecord {
                    group_index,
                    node_members: GroupMembers::new(),
                    edge_members: GroupMembers::new(),
                },
            );

        self.group_count += 1;

        Some(group_address)
    }

    #[cfg(feature = "serde")]
    pub(crate) fn rebuild_indices(&mut self) {
        let hashed_addresses: Vec<_> = self
            .iter()
            .map(|(group_address, record)| {
                (
                    self.hash_group_index(&record.group_index),
                    group_address.index(),
                )
            })
            .collect();

        for (hash, address_index) in hashed_addresses {
            self.indices.insert(hash, address_index);
        }
    }

    pub fn remove(&mut self, group_address: GroupAddress) -> Option<Arc<GroupRecord>> {
        let chunk_index = group_address.chunk_index();
        let chunk_local_address = group_address.chunk_local_address();
        let record = self
            .records
            .get(chunk_index)?
            .get(chunk_local_address)?
            .clone();

        let hash = self.hash_group_index(&record.group_index);
        self.indices.remove(hash, group_address.index());

        let chunk = self
            .records
            .get_mut(chunk_index)
            .expect("Chunk must exist.");
        chunk.remove(chunk_local_address);

        if chunk.is_empty() {
            self.records.remove_chunk(chunk_index);
        }

        self.group_count -= 1;

        Some(record)
    }

    pub fn iter(&self) -> impl Iterator<Item = (GroupAddress, &Arc<GroupRecord>)> + '_ {
        self.records.chunks().flat_map(|(chunk_index, chunk)| {
            chunk.iter().map(move |(chunk_local_address, record)| {
                (
                    GroupAddress::from_chunk_parts(chunk_index, chunk_local_address),
                    record,
                )
            })
        })
    }

    pub const fn group_count(&self) -> usize {
        self.group_count
    }

    pub const fn ungrouped_node_count(&self) -> usize {
        self.ungrouped_node_count
    }

    pub const fn ungrouped_edge_count(&self) -> usize {
        self.ungrouped_edge_count
    }

    pub const fn increment_ungrouped_node_count(&mut self) {
        self.ungrouped_node_count += 1;
    }

    pub const fn decrement_ungrouped_node_count(&mut self) {
        self.ungrouped_node_count -= 1;
    }

    pub const fn increment_ungrouped_edge_count(&mut self) {
        self.ungrouped_edge_count += 1;
    }

    pub const fn decrement_ungrouped_edge_count(&mut self) {
        self.ungrouped_edge_count -= 1;
    }

    fn hash_group_index(&self, group_index: &GroupIndex) -> u64 {
        self.indices
            .hash_one(IdentifierView::from(group_index.identifier()))
    }
}

impl Default for GroupDirectory {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Default)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct GroupChunk {
    present: PresenceBitmap,
    cells: Vec<Arc<GroupRecord>>,
}

impl GroupChunk {
    pub fn get(&self, cell_index: usize) -> Option<&Arc<GroupRecord>> {
        if !self.present.contains(cell_index) {
            return None;
        }

        Some(&self.cells[self.present.rank(cell_index)])
    }

    pub fn get_mut(&mut self, cell_index: usize) -> Option<&mut GroupRecord> {
        if !self.present.contains(cell_index) {
            return None;
        }

        let rank = self.present.rank(cell_index);
        Some(Arc::make_mut(&mut self.cells[rank]))
    }

    pub fn set(&mut self, cell_index: usize, record: GroupRecord) {
        if self.present.contains(cell_index) {
            let rank = self.present.rank(cell_index);
            self.cells[rank] = Arc::new(record);
            return;
        }

        let rank = self.present.set(cell_index);
        self.cells.insert(rank, Arc::new(record));
    }

    pub fn remove(&mut self, cell_index: usize) -> bool {
        if !self.present.contains(cell_index) {
            return false;
        }

        let rank = self.present.clear(cell_index);
        self.cells.remove(rank);

        true
    }

    pub const fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, &Arc<GroupRecord>)> + '_ {
        self.present.iter_present().zip(self.cells.iter())
    }
}

#[derive(Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct GroupRecord {
    pub group_index: GroupIndex,
    pub node_members: GroupMembers,
    pub edge_members: GroupMembers,
}

#[derive(Clone, Default)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct GroupMembers {
    members: ChunkTree<PresenceBitmap>,
    member_count: usize,
}

impl GroupMembers {
    pub const fn new() -> Self {
        Self {
            members: ChunkTree::new(),
            member_count: 0,
        }
    }

    #[allow(dead_code)]
    pub fn contains(&self, address_index: u32) -> bool {
        let (chunk_index, chunk_local_address) = Self::locate(address_index);

        self.members
            .get(chunk_index)
            .is_some_and(|chunk| chunk.contains(chunk_local_address))
    }

    pub fn insert(&mut self, address_index: u32) -> bool {
        let (chunk_index, chunk_local_address) = Self::locate(address_index);
        let chunk = self.members.get_mut_or_default(chunk_index);

        if chunk.contains(chunk_local_address) {
            return false;
        }

        chunk.set(chunk_local_address);
        self.member_count += 1;

        true
    }

    pub fn remove(&mut self, address_index: u32) -> bool {
        let (chunk_index, chunk_local_address) = Self::locate(address_index);

        let Some(chunk) = self.members.get_mut(chunk_index) else {
            return false;
        };

        if !chunk.contains(chunk_local_address) {
            return false;
        }

        chunk.clear(chunk_local_address);
        self.member_count -= 1;

        if chunk.is_empty() {
            self.members.remove_chunk(chunk_index);
        }

        true
    }

    pub const fn len(&self) -> usize {
        self.member_count
    }

    pub const fn is_empty(&self) -> bool {
        self.member_count == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.members.chunks().flat_map(|(chunk_index, chunk)| {
            chunk.iter_present().map(move |chunk_local_address| {
                Self::index_from_chunk_parts(chunk_index, chunk_local_address)
            })
        })
    }

    const fn locate(address_index: u32) -> (u32, usize) {
        let chunk_index = address_index >> CHUNK_LOCAL_ADDRESS_BITS;
        let chunk_local_address = (address_index as usize) & (ADDRESSES_PER_CHUNK - 1);

        (chunk_index, chunk_local_address)
    }

    const fn index_from_chunk_parts(chunk_index: u32, chunk_local_address: usize) -> u32 {
        (chunk_index << CHUNK_LOCAL_ADDRESS_BITS) + chunk_local_address as u32
    }
}

#[cfg(test)]
mod test {
    use super::{GroupChunk, GroupDirectory, GroupMembers, GroupRecord};
    use crate::graphrecord::{datatypes::GroupIndex, state::GroupAddress};

    fn create_directory_with_lorem() -> (GroupDirectory, GroupAddress) {
        let mut directory = GroupDirectory::new();
        let address = directory.add(GroupIndex::from("lorem")).unwrap();

        (directory, address)
    }

    fn create_group_record(name: &str) -> GroupRecord {
        GroupRecord {
            group_index: GroupIndex::from(name),
            node_members: GroupMembers::new(),
            edge_members: GroupMembers::new(),
        }
    }

    fn create_members() -> GroupMembers {
        let mut members = GroupMembers::new();
        members.insert(255);
        members.insert(256);
        members.insert(300);

        members
    }

    #[test]
    fn test_group_directory_new() {
        assert_eq!(0, GroupDirectory::new().group_count());
        assert_eq!(0, GroupDirectory::default().group_count());
    }

    #[test]
    fn test_group_directory_resolve() {
        let (directory, address) = create_directory_with_lorem();

        assert_eq!(Some(address), directory.resolve(&GroupIndex::from("lorem")));
    }

    #[test]
    fn test_invalid_group_directory_resolve() {
        let directory = create_directory_with_lorem().0;

        assert_eq!(None, directory.resolve(&GroupIndex::from("missing")));
    }

    #[test]
    fn test_group_directory_record() {
        let (directory, address) = create_directory_with_lorem();

        assert_eq!(
            &GroupIndex::from("lorem"),
            &directory.record(address).unwrap().group_index
        );
    }

    #[test]
    fn test_invalid_group_directory_record() {
        let (mut directory, address) = create_directory_with_lorem();
        directory.remove(address).unwrap();

        assert!(directory.record(address).is_none());
    }

    #[test]
    fn test_group_directory_record_mut() {
        let (mut directory, address) = create_directory_with_lorem();

        let record = directory.record_mut(address).unwrap();
        record.node_members.insert(42);

        assert!(directory.record(address).unwrap().node_members.contains(42));
    }

    #[test]
    fn test_invalid_group_directory_record_mut() {
        let mut directory = GroupDirectory::new();

        assert!(directory.record_mut(GroupAddress::new(999)).is_none());
    }

    #[test]
    fn test_group_directory_group_index() {
        let (directory, address) = create_directory_with_lorem();

        assert_eq!(
            Some(&GroupIndex::from("lorem")),
            directory.group_index(address)
        );
    }

    #[test]
    fn test_invalid_group_directory_group_index() {
        let directory = create_directory_with_lorem().0;

        assert_eq!(None, directory.group_index(GroupAddress::new(999)));
    }

    #[test]
    fn test_group_directory_add() {
        let mut directory = GroupDirectory::new();
        let first = directory.add(GroupIndex::from("lorem")).unwrap();
        let second = directory.add(GroupIndex::from("ipsum")).unwrap();

        assert_ne!(first, second);
        assert_eq!(2, directory.group_count());

        let first_address = directory.add(GroupIndex::from("dolor")).unwrap();
        directory.remove(first_address).unwrap();

        let second_address = directory.add(GroupIndex::from("dolor")).unwrap();

        assert_ne!(first_address, second_address);
        assert_eq!(
            Some(second_address),
            directory.resolve(&GroupIndex::from("dolor"))
        );
    }

    #[test]
    fn test_invalid_group_directory_add() {
        let mut directory = create_directory_with_lorem().0;

        assert!(directory.add(GroupIndex::from("lorem")).is_none());
        assert_eq!(1, directory.group_count());
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_group_directory_rebuild_indices() {
        let (mut directory, address) = create_directory_with_lorem();

        directory.indices = super::KeyDictionary::new();
        directory.rebuild_indices();

        assert_eq!(Some(address), directory.resolve(&GroupIndex::from("lorem")));
    }

    #[test]
    fn test_group_directory_remove() {
        let (mut directory, address) = create_directory_with_lorem();

        let removed = directory.remove(address).unwrap();

        assert_eq!(GroupIndex::from("lorem"), removed.group_index);
        assert_eq!(None, directory.resolve(&GroupIndex::from("lorem")));
        assert!(directory.record(address).is_none());
        assert_eq!(0, directory.group_count());
    }

    #[test]
    fn test_group_directory_iter() {
        let mut directory = GroupDirectory::new();
        let first = directory.add(GroupIndex::from("lorem")).unwrap();
        let second = directory.add(GroupIndex::from("ipsum")).unwrap();

        let mut observed: Vec<_> = directory
            .iter()
            .map(|(address, record)| (address, record.group_index.clone()))
            .collect();
        observed.sort_by_key(|(address, _)| address.index());

        assert_eq!(
            vec![
                (first, GroupIndex::from("lorem")),
                (second, GroupIndex::from("ipsum"))
            ],
            observed
        );
    }

    #[test]
    fn test_group_directory_group_count() {
        let mut directory = GroupDirectory::new();

        assert_eq!(0, directory.group_count());

        directory.add(GroupIndex::from("lorem")).unwrap();

        assert_eq!(1, directory.group_count());
    }

    #[test]
    fn test_group_directory_ungrouped_node_count() {
        let directory = GroupDirectory::new();

        assert_eq!(0, directory.ungrouped_node_count());
    }

    #[test]
    fn test_group_directory_ungrouped_edge_count() {
        let directory = GroupDirectory::new();

        assert_eq!(0, directory.ungrouped_edge_count());
    }

    #[test]
    fn test_group_directory_increment_ungrouped_node_count() {
        let mut directory = GroupDirectory::new();

        directory.increment_ungrouped_node_count();
        directory.increment_ungrouped_node_count();

        assert_eq!(2, directory.ungrouped_node_count());
    }

    #[test]
    fn test_group_directory_decrement_ungrouped_node_count() {
        let mut directory = GroupDirectory::new();
        directory.increment_ungrouped_node_count();
        directory.increment_ungrouped_node_count();

        directory.decrement_ungrouped_node_count();

        assert_eq!(1, directory.ungrouped_node_count());
    }

    #[test]
    fn test_group_directory_increment_ungrouped_edge_count() {
        let mut directory = GroupDirectory::new();

        directory.increment_ungrouped_edge_count();

        assert_eq!(1, directory.ungrouped_edge_count());
    }

    #[test]
    fn test_group_directory_decrement_ungrouped_edge_count() {
        let mut directory = GroupDirectory::new();
        directory.increment_ungrouped_edge_count();

        directory.decrement_ungrouped_edge_count();

        assert_eq!(0, directory.ungrouped_edge_count());
    }

    #[test]
    fn test_group_chunk_get() {
        let chunk = GroupChunk::default();

        assert!(chunk.get(0).is_none());

        let mut chunk = GroupChunk::default();
        chunk.set(5, create_group_record("lorem"));

        assert_eq!(
            &GroupIndex::from("lorem"),
            &chunk.get(5).unwrap().group_index
        );
    }

    #[test]
    fn test_group_chunk_get_mut() {
        let mut chunk = GroupChunk::default();
        chunk.set(5, create_group_record("lorem"));

        chunk.get_mut(5).unwrap().node_members.insert(42);

        assert!(chunk.get(5).unwrap().node_members.contains(42));
    }

    #[test]
    fn test_invalid_group_chunk_get_mut() {
        let mut chunk = GroupChunk::default();

        assert!(chunk.get_mut(0).is_none());
    }

    #[test]
    fn test_group_chunk_set() {
        let mut chunk = GroupChunk::default();

        chunk.set(5, create_group_record("lorem"));

        assert_eq!(
            &GroupIndex::from("lorem"),
            &chunk.get(5).unwrap().group_index
        );

        chunk.set(5, create_group_record("ipsum"));

        assert_eq!(
            &GroupIndex::from("ipsum"),
            &chunk.get(5).unwrap().group_index
        );
    }

    #[test]
    fn test_group_chunk_remove() {
        let mut chunk = GroupChunk::default();
        chunk.set(5, create_group_record("lorem"));

        assert!(chunk.remove(5));

        assert!(chunk.get(5).is_none());
    }

    #[test]
    fn test_invalid_group_chunk_remove() {
        let mut chunk = GroupChunk::default();

        assert!(!chunk.remove(5));
    }

    #[test]
    fn test_group_chunk_is_empty() {
        let mut chunk = GroupChunk::default();

        assert!(chunk.is_empty());

        chunk.set(5, create_group_record("lorem"));

        assert!(!chunk.is_empty());

        chunk.remove(5);

        assert!(chunk.is_empty());
    }

    #[test]
    fn test_group_chunk_iter() {
        let mut chunk = GroupChunk::default();
        chunk.set(5, create_group_record("lorem"));
        chunk.set(2, create_group_record("ipsum"));

        let observed: Vec<_> = chunk
            .iter()
            .map(|(cell_index, record)| (cell_index, record.group_index.clone()))
            .collect();

        assert_eq!(
            vec![
                (2, GroupIndex::from("ipsum")),
                (5, GroupIndex::from("lorem"))
            ],
            observed
        );
    }

    #[test]
    fn test_group_members_new() {
        assert_eq!(0, GroupMembers::new().len());
        assert_eq!(0, GroupMembers::default().len());
    }

    #[test]
    fn test_group_members_contains() {
        let members = create_members();

        assert!(members.contains(255));
        assert!(members.contains(256));
        assert!(members.contains(300));
        assert!(!members.contains(0));
    }

    #[test]
    fn test_group_members_insert() {
        let mut members = GroupMembers::new();

        assert!(members.insert(255));
        assert!(members.insert(256));
        assert!(members.insert(300));
        assert_eq!(3, members.len());
    }

    #[test]
    fn test_group_members_remove() {
        let mut members = create_members();

        assert!(members.remove(256));

        assert_eq!(vec![255, 300], members.iter().collect::<Vec<_>>());
        assert_eq!(2, members.len());
    }

    #[test]
    fn test_invalid_group_members_remove() {
        let mut members = create_members();
        members.remove(256);

        assert!(!members.remove(256));
    }

    #[test]
    fn test_group_members_len() {
        let mut members = GroupMembers::new();

        assert_eq!(0, members.len());

        members.insert(255);

        assert_eq!(1, members.len());
    }

    #[test]
    fn test_group_members_is_empty() {
        let members = GroupMembers::new();

        assert!(members.is_empty());
    }

    #[test]
    fn test_group_members_iter() {
        let members = create_members();

        assert_eq!(vec![255, 256, 300], members.iter().collect::<Vec<_>>());
    }
}
