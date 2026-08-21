use super::{GroupAddress, presence::PresenceBitmap};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub enum Memberships {
    One(GroupAddress),
    Several(Arc<[GroupAddress]>),
}

impl Memberships {
    pub fn iter(&self) -> impl Iterator<Item = GroupAddress> + '_ {
        match self {
            Self::One(group_address) => MembershipsIterator::One(std::iter::once(*group_address)),
            Self::Several(group_addresses) => {
                MembershipsIterator::Several(group_addresses.iter().copied())
            }
        }
    }

    #[allow(dead_code)]
    pub fn contains(&self, group_address: GroupAddress) -> bool {
        match self {
            Self::One(existing) => *existing == group_address,
            Self::Several(group_addresses) => group_addresses.contains(&group_address),
        }
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct MembershipChunk {
    present: PresenceBitmap,
    cells: Vec<Memberships>,
}

impl MembershipChunk {
    pub const fn new() -> Self {
        Self {
            present: PresenceBitmap::new(),
            cells: Vec::new(),
        }
    }

    pub fn get(&self, cell_index: usize) -> Option<&Memberships> {
        if !self.present.contains(cell_index) {
            return None;
        }

        Some(&self.cells[self.present.rank(cell_index)])
    }

    #[allow(dead_code)]
    pub fn contains(&self, cell_index: usize, group_address: GroupAddress) -> bool {
        self.get(cell_index)
            .is_some_and(|memberships| memberships.contains(group_address))
    }

    pub fn memberships(&self, cell_index: usize) -> impl Iterator<Item = GroupAddress> + '_ {
        self.get(cell_index).into_iter().flat_map(Memberships::iter)
    }

    pub fn add(&mut self, cell_index: usize, group_address: GroupAddress) -> bool {
        if !self.present.contains(cell_index) {
            let rank = self.present.set(cell_index);
            self.cells.insert(rank, Memberships::One(group_address));
            return true;
        }

        let rank = self.present.rank(cell_index);
        match &self.cells[rank] {
            Memberships::One(existing) => {
                if *existing == group_address {
                    return false;
                }

                let mut members = vec![*existing, group_address];
                members.sort_by_key(GroupAddress::index);

                self.cells[rank] = Memberships::Several(Arc::from(members));

                true
            }
            Memberships::Several(group_addresses) => {
                if group_addresses.contains(&group_address) {
                    return false;
                }

                let mut members: Vec<_> = group_addresses.iter().copied().collect();
                members.push(group_address);
                members.sort_by_key(GroupAddress::index);

                self.cells[rank] = Memberships::Several(Arc::from(members));

                true
            }
        }
    }

    pub fn remove(&mut self, cell_index: usize, group_address: GroupAddress) -> bool {
        if !self.present.contains(cell_index) {
            return false;
        }

        let rank = self.present.rank(cell_index);
        match &self.cells[rank] {
            Memberships::One(existing) => {
                if *existing != group_address {
                    return false;
                }

                self.present.clear(cell_index);
                self.cells.remove(rank);

                true
            }
            Memberships::Several(group_addresses) => {
                if !group_addresses.contains(&group_address) {
                    return false;
                }

                let remaining: Vec<_> = group_addresses
                    .iter()
                    .copied()
                    .filter(|candidate| *candidate != group_address)
                    .collect();

                self.cells[rank] = if remaining.len() == 1 {
                    Memberships::One(remaining[0])
                } else {
                    Memberships::Several(Arc::from(remaining))
                };

                true
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }
}

impl Default for MembershipChunk {
    fn default() -> Self {
        Self::new()
    }
}

enum MembershipsIterator<'a> {
    One(std::iter::Once<GroupAddress>),
    Several(std::iter::Copied<std::slice::Iter<'a, GroupAddress>>),
}

impl Iterator for MembershipsIterator<'_> {
    type Item = GroupAddress;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(iterator) => iterator.next(),
            Self::Several(iterator) => iterator.next(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{MembershipChunk, Memberships};
    use crate::graphrecord::state::GroupAddress;

    fn create_group(index: u32) -> GroupAddress {
        GroupAddress::new(index)
    }

    #[test]
    fn test_memberships_iter() {
        let one = Memberships::One(create_group(4));

        assert_eq!(vec![create_group(4)], one.iter().collect::<Vec<_>>());

        let several = Memberships::Several(std::sync::Arc::from(vec![
            create_group(1),
            create_group(2),
            create_group(3),
        ]));

        assert_eq!(
            vec![create_group(1), create_group(2), create_group(3)],
            several.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_memberships_contains() {
        let one = Memberships::One(create_group(4));

        assert!(one.contains(create_group(4)));
        assert!(!one.contains(create_group(5)));

        let several = Memberships::Several(std::sync::Arc::from(vec![
            create_group(1),
            create_group(2),
            create_group(3),
        ]));

        assert!(several.contains(create_group(2)));
        assert!(!several.contains(create_group(9)));
    }

    #[test]
    fn test_membership_chunk_new() {
        let chunk = MembershipChunk::new();

        assert!(chunk.is_empty());
        assert!(MembershipChunk::default().is_empty());
    }

    #[test]
    fn test_membership_chunk_get() {
        let chunk = MembershipChunk::new();

        assert!(chunk.get(0).is_none());

        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(1));

        assert_eq!(Some(&Memberships::One(create_group(1))), chunk.get(5));
    }

    #[test]
    fn test_membership_chunk_contains() {
        let chunk = MembershipChunk::new();

        assert!(!chunk.contains(0, create_group(1)));

        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(1));

        assert!(chunk.contains(5, create_group(1)));
    }

    #[test]
    fn test_membership_chunk_memberships() {
        let chunk = MembershipChunk::new();

        assert_eq!(0, chunk.memberships(0).count());

        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(9));
        chunk.add(5, create_group(2));

        assert_eq!(
            vec![create_group(2), create_group(9)],
            chunk.memberships(5).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_membership_chunk_add() {
        let mut chunk = MembershipChunk::new();

        assert!(chunk.add(5, create_group(1)));

        assert_eq!(Some(&Memberships::One(create_group(1))), chunk.get(5));

        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(9));

        assert!(chunk.add(5, create_group(2)));

        let Some(Memberships::Several(members)) = chunk.get(5) else {
            panic!("Memberships must be Several.");
        };

        assert_eq!(&[create_group(2), create_group(9)], members.as_ref());

        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(9));
        chunk.add(5, create_group(2));

        assert!(chunk.add(5, create_group(5)));

        let Some(Memberships::Several(members)) = chunk.get(5) else {
            panic!("Memberships must be Several.");
        };

        assert_eq!(
            &[create_group(2), create_group(5), create_group(9)],
            members.as_ref()
        );
    }

    #[test]
    fn test_invalid_membership_chunk_add() {
        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(1));

        assert!(!chunk.add(5, create_group(1)));

        assert_eq!(Some(&Memberships::One(create_group(1))), chunk.get(5));

        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(9));
        chunk.add(5, create_group(2));

        assert!(!chunk.add(5, create_group(2)));

        let Some(Memberships::Several(members)) = chunk.get(5) else {
            panic!("Memberships must be Several.");
        };

        assert_eq!(&[create_group(2), create_group(9)], members.as_ref());
    }

    #[test]
    fn test_membership_chunk_remove() {
        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(9));
        chunk.add(5, create_group(2));

        assert!(chunk.remove(5, create_group(9)));

        assert_eq!(Some(&Memberships::One(create_group(2))), chunk.get(5));

        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(2));

        assert!(chunk.remove(5, create_group(2)));

        assert!(chunk.get(5).is_none());
        assert!(chunk.is_empty());
    }

    #[test]
    fn test_invalid_membership_chunk_remove() {
        let mut chunk = MembershipChunk::new();

        chunk.add(5, create_group(2));

        assert!(!chunk.remove(5, create_group(3)));
        assert!(!chunk.remove(6, create_group(2)));
    }

    #[test]
    fn test_membership_chunk_is_empty() {
        let mut chunk = MembershipChunk::new();

        assert!(chunk.is_empty());

        chunk.add(5, create_group(1));

        assert!(!chunk.is_empty());

        assert!(chunk.remove(5, create_group(1)));

        assert!(chunk.is_empty());
    }
}
