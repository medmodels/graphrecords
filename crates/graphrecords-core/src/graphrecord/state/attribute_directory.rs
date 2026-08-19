use super::{
    ADDRESSES_PER_CHUNK, AttributeAddress, CHUNK_LOCAL_ADDRESS_BITS,
    attribute_chunk::AttributeChunk, chunk_tree::ChunkTree,
};
use crate::graphrecord::datatypes::{AttributeName, Identifier, Value};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct AttributeDirectory {
    attributes: Vec<(AttributeName, ChunkTree<AttributeChunk>)>,
}

impl AttributeDirectory {
    pub const fn new() -> Self {
        Self {
            attributes: Vec::new(),
        }
    }

    pub fn resolve(&self, name: &AttributeName) -> Option<AttributeAddress> {
        self.attributes
            .binary_search_by(|(candidate, _)| Self::compare_names(candidate, name))
            .ok()
            .map(|index| AttributeAddress::new(index as u32))
    }

    pub fn name(&self, address: AttributeAddress) -> Option<&AttributeName> {
        self.attributes
            .get(address.index() as usize)
            .map(|(name, _)| name)
    }

    pub fn chunk_tree(&self, address: AttributeAddress) -> Option<&ChunkTree<AttributeChunk>> {
        self.attributes
            .get(address.index() as usize)
            .map(|(_, chunk_tree)| chunk_tree)
    }

    pub fn chunk_tree_mut(
        &mut self,
        address: AttributeAddress,
    ) -> Option<&mut ChunkTree<AttributeChunk>> {
        self.attributes
            .get_mut(address.index() as usize)
            .map(|(_, chunk_tree)| chunk_tree)
    }

    pub fn resolve_or_insert(&mut self, name: &AttributeName) -> AttributeAddress {
        match self
            .attributes
            .binary_search_by(|(candidate, _)| Self::compare_names(candidate, name))
        {
            Ok(index) => AttributeAddress::new(index as u32),
            Err(insertion_index) => {
                self.attributes
                    .insert(insertion_index, (name.clone(), ChunkTree::new()));

                AttributeAddress::new(insertion_index as u32)
            }
        }
    }

    pub fn set(&mut self, name: &AttributeName, entity_address_index: u32, value: &Value) {
        let attribute_address = self.resolve_or_insert(name);
        let (chunk_index, chunk_local_address) = Self::locate(entity_address_index);
        let chunk_tree = &mut self.attributes[attribute_address.index() as usize].1;

        chunk_tree
            .get_mut_or_insert_with(chunk_index, || AttributeChunk::new(value))
            .set(chunk_local_address, value);
    }

    pub fn remove_value(
        &mut self,
        attribute_address: AttributeAddress,
        entity_address_index: u32,
    ) -> bool {
        let Some(chunk_tree) = self.chunk_tree_mut(attribute_address) else {
            return false;
        };

        let (chunk_index, chunk_local_address) = Self::locate(entity_address_index);

        let Some(chunk) = chunk_tree.get_mut(chunk_index) else {
            return false;
        };

        if !chunk.remove(chunk_local_address) {
            return false;
        }

        if chunk.is_empty() {
            chunk_tree.remove_chunk(chunk_index);
        }

        true
    }

    pub fn prune_empty(&mut self) {
        self.attributes
            .retain(|(_, chunk_tree)| !chunk_tree.is_empty());
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (AttributeAddress, &AttributeName, &ChunkTree<AttributeChunk>)> + '_
    {
        self.attributes
            .iter()
            .enumerate()
            .map(|(index, (name, chunk_tree))| {
                (AttributeAddress::new(index as u32), name, chunk_tree)
            })
    }

    const fn locate(address_index: u32) -> (u32, usize) {
        let chunk_index = address_index >> CHUNK_LOCAL_ADDRESS_BITS;
        let chunk_local_address = (address_index as usize) & (ADDRESSES_PER_CHUNK - 1);

        (chunk_index, chunk_local_address)
    }

    fn compare_names(left: &AttributeName, right: &AttributeName) -> Ordering {
        match (left.identifier(), right.identifier()) {
            (Identifier::Int(left_value), Identifier::Int(right_value)) => {
                left_value.cmp(right_value)
            }
            (Identifier::String(left_value), Identifier::String(right_value)) => {
                left_value.cmp(right_value)
            }
            (Identifier::Int(_), Identifier::String(_)) => Ordering::Less,
            (Identifier::String(_), Identifier::Int(_)) => Ordering::Greater,
        }
    }
}

impl Default for AttributeDirectory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::{AttributeChunk, AttributeDirectory};
    use crate::graphrecord::{
        datatypes::{AttributeName, Value, ValueView},
        state::AttributeAddress,
    };

    fn create_directory_with_two_attributes() -> AttributeDirectory {
        let mut directory = AttributeDirectory::new();

        directory.resolve_or_insert(&AttributeName::from("lorem"));
        directory.resolve_or_insert(&AttributeName::from("ipsum"));

        directory
    }

    fn create_directory_with_lorem() -> (AttributeDirectory, AttributeAddress) {
        let mut directory = AttributeDirectory::new();
        let address = directory.resolve_or_insert(&AttributeName::from("lorem"));

        (directory, address)
    }

    #[test]
    fn test_new() {
        assert_eq!(0, AttributeDirectory::new().iter().count());
        assert_eq!(0, AttributeDirectory::default().iter().count());
    }

    #[test]
    fn test_resolve() {
        let (directory, address) = create_directory_with_lorem();

        assert_eq!(
            Some(address),
            directory.resolve(&AttributeName::from("lorem"))
        );
    }

    #[test]
    fn test_invalid_resolve() {
        let directory = AttributeDirectory::new();

        assert_eq!(None, directory.resolve(&AttributeName::from("lorem")));
    }

    #[test]
    fn test_name() {
        let (directory, address) = create_directory_with_lorem();

        assert_eq!(Some(&AttributeName::from("lorem")), directory.name(address));
    }

    #[test]
    fn test_invalid_name() {
        let directory = create_directory_with_lorem().0;

        assert_eq!(None, directory.name(AttributeAddress::new(999)));
    }

    #[test]
    fn test_chunk_tree() {
        let (directory, address) = create_directory_with_lorem();

        assert!(directory.chunk_tree(address).is_some());
    }

    #[test]
    fn test_invalid_chunk_tree() {
        let directory = create_directory_with_lorem().0;

        assert!(directory.chunk_tree(AttributeAddress::new(999)).is_none());
    }

    #[test]
    fn test_chunk_tree_mut() {
        let (mut directory, address) = create_directory_with_lorem();

        assert!(directory.chunk_tree_mut(address).is_some());
    }

    #[test]
    fn test_invalid_chunk_tree_mut() {
        let mut directory = create_directory_with_lorem().0;

        assert!(
            directory
                .chunk_tree_mut(AttributeAddress::new(999))
                .is_none()
        );
    }

    #[test]
    fn test_resolve_or_insert() {
        let mut directory = AttributeDirectory::new();

        let first_address = directory.resolve_or_insert(&AttributeName::from("lorem"));
        let second_address = directory.resolve_or_insert(&AttributeName::from("lorem"));

        assert_eq!(first_address, second_address);
        assert_eq!(1, directory.iter().count());

        let mut directory = AttributeDirectory::new();

        directory.resolve_or_insert(&AttributeName::from("sed"));
        directory.resolve_or_insert(&AttributeName::from("dolor"));
        directory.resolve_or_insert(&AttributeName::from("ipsum"));

        let observed: Vec<_> = directory.iter().map(|(_, name, _)| name.clone()).collect();

        assert_eq!(
            vec![
                AttributeName::from("dolor"),
                AttributeName::from("ipsum"),
                AttributeName::from("sed"),
            ],
            observed
        );

        let mut directory = AttributeDirectory::new();

        directory.resolve_or_insert(&AttributeName::from(5));
        directory.resolve_or_insert(&AttributeName::from("lorem"));
        directory.resolve_or_insert(&AttributeName::from(1));

        let observed: Vec<_> = directory.iter().map(|(_, name, _)| name.clone()).collect();

        assert_eq!(
            vec![
                AttributeName::from(1),
                AttributeName::from(5),
                AttributeName::from("lorem"),
            ],
            observed
        );
    }

    #[test]
    fn test_set() {
        let mut directory = AttributeDirectory::new();
        let name = AttributeName::from("lorem");

        directory.set(&name, 5, &Value::Int(42));

        let attribute_address = directory.resolve(&name).unwrap();

        assert_eq!(
            Some(ValueView::Int(42)),
            directory
                .chunk_tree(attribute_address)
                .unwrap()
                .get(0)
                .unwrap()
                .get(5)
        );

        let mut directory = AttributeDirectory::new();
        let name = AttributeName::from("lorem");

        directory.set(&name, 0, &Value::Int(1));
        directory.set(&name, 1, &Value::String("ipsum".to_string()));

        let attribute_address = directory.resolve(&name).unwrap();
        let chunk = directory
            .chunk_tree(attribute_address)
            .unwrap()
            .get(0)
            .unwrap();

        assert!(matches!(chunk, AttributeChunk::Mixed(_)));
        assert_eq!(Some(ValueView::Int(1)), chunk.get(0));
        assert_eq!(Some(ValueView::String("ipsum".into())), chunk.get(1));
    }

    #[test]
    fn test_remove_value() {
        let mut directory = AttributeDirectory::new();
        let name = AttributeName::from("lorem");

        directory.set(&name, 5, &Value::Int(42));
        directory.set(&name, 6, &Value::Int(43));

        let attribute_address = directory.resolve(&name).unwrap();

        assert!(directory.remove_value(attribute_address, 5));
        assert!(
            directory
                .chunk_tree(attribute_address)
                .unwrap()
                .get(0)
                .is_some()
        );

        assert!(directory.remove_value(attribute_address, 6));
        assert!(
            directory
                .chunk_tree(attribute_address)
                .unwrap()
                .get(0)
                .is_none()
        );

        directory.prune_empty();

        assert_eq!(0, directory.iter().count());
        assert_eq!(None, directory.resolve(&name));
    }

    #[test]
    fn test_invalid_remove_value() {
        let mut directory = AttributeDirectory::new();
        let name = AttributeName::from("lorem");

        directory.set(&name, 5, &Value::Int(42));

        let attribute_address = directory.resolve(&name).unwrap();

        assert!(!directory.remove_value(AttributeAddress::new(999), 5));
        assert!(!directory.remove_value(attribute_address, 999));
        assert!(!directory.remove_value(attribute_address, 7));
    }

    #[test]
    fn test_prune_empty() {
        let mut directory = create_directory_with_two_attributes();

        assert_eq!(2, directory.iter().count());

        directory.prune_empty();

        assert_eq!(0, directory.iter().count());
        assert_eq!(None, directory.resolve(&AttributeName::from("lorem")));
        assert_eq!(None, directory.resolve(&AttributeName::from("ipsum")));
    }

    #[test]
    fn test_iter() {
        let directory = create_directory_with_two_attributes();

        for (address, name, _) in directory.iter() {
            assert_eq!(Some(address), directory.resolve(name));
        }

        assert_eq!(2, directory.iter().count());
    }
}
