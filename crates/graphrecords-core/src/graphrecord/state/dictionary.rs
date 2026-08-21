use std::{
    hash::{BuildHasher, Hash, RandomState},
    sync::Arc,
};

const BITS_PER_LEVEL: u32 = 6;
const LEVEL_MASK: u64 = (1 << BITS_PER_LEVEL) - 1;

#[derive(Clone, Default)]
pub struct KeyDictionary {
    root: Option<Arc<Node>>,
    hash_builder: RandomState,
}

impl KeyDictionary {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn hash_one(&self, value: impl Hash) -> u64 {
        self.hash_builder.hash_one(value)
    }

    pub fn candidates(&self, hash: u64) -> impl Iterator<Item = u32> + '_ {
        self.find_leaf(hash).into_iter().flatten().copied()
    }

    pub fn insert(&mut self, hash: u64, address_index: u32) -> bool {
        match self.root.take() {
            None => {
                self.root = Some(Arc::new(Node::Leaf(hash, vec![address_index])));

                true
            }
            Some(mut node) => {
                let was_inserted = Self::insert_into_subtree(&mut node, 0, hash, address_index);

                self.root = Some(node);

                was_inserted
            }
        }
    }

    pub fn remove(&mut self, hash: u64, address_index: u32) -> bool {
        let Some(mut node) = self.root.take() else {
            return false;
        };

        let (was_removed, now_empty) = Self::remove_from_subtree(&mut node, 0, hash, address_index);

        if !now_empty {
            self.root = Some(node);
        }

        was_removed
    }

    const fn child_index(hash: u64, level: u32) -> u32 {
        ((hash >> (level * BITS_PER_LEVEL)) & LEVEL_MASK) as u32
    }

    fn find_leaf(&self, hash: u64) -> Option<&[u32]> {
        let mut node = self.root.as_deref()?;
        let mut level = 0u32;

        loop {
            match node {
                Node::Leaf(leaf_hash, address_indices) => {
                    return if *leaf_hash == hash {
                        Some(address_indices.as_slice())
                    } else {
                        None
                    };
                }
                Node::Branch(branch) => {
                    let index = Self::child_index(hash, level);
                    node = branch.child(index)?.as_ref();
                    level += 1;
                }
            }
        }
    }

    fn insert_into_subtree(
        node: &mut Arc<Node>,
        level: u32,
        hash: u64,
        address_index: u32,
    ) -> bool {
        match Arc::make_mut(node) {
            Node::Leaf(leaf_hash, address_indices) if *leaf_hash == hash => {
                if address_indices.contains(&address_index) {
                    return false;
                }

                address_indices.push(address_index);

                true
            }
            Node::Leaf(leaf_hash, address_indices) => {
                let existing_hash = *leaf_hash;
                let existing_address_indices = std::mem::take(address_indices);
                let split = Self::build_split(
                    existing_hash,
                    existing_address_indices,
                    hash,
                    address_index,
                    level,
                );

                *node = Arc::new(split);

                true
            }
            Node::Branch(branch) => branch.insert(level, hash, address_index),
        }
    }

    fn remove_from_subtree(
        node: &mut Arc<Node>,
        level: u32,
        hash: u64,
        address_index: u32,
    ) -> (bool, bool) {
        match Arc::make_mut(node) {
            Node::Leaf(leaf_hash, address_indices) => {
                if *leaf_hash != hash {
                    return (false, false);
                }

                match address_indices
                    .iter()
                    .position(|candidate| *candidate == address_index)
                {
                    None => (false, false),
                    Some(index) => {
                        address_indices.remove(index);
                        (true, address_indices.is_empty())
                    }
                }
            }
            Node::Branch(branch) => branch.remove(level, hash, address_index),
        }
    }

    fn build_split(
        existing_hash: u64,
        existing_address_indices: Vec<u32>,
        new_hash: u64,
        new_address_index: u32,
        level: u32,
    ) -> Node {
        let existing_index = Self::child_index(existing_hash, level);
        let new_index = Self::child_index(new_hash, level);

        if existing_index != new_index {
            let existing_leaf = Arc::new(Node::Leaf(existing_hash, existing_address_indices));
            let new_leaf = Arc::new(Node::Leaf(new_hash, vec![new_address_index]));
            let branch = Branch::pair(existing_index, existing_leaf, new_index, new_leaf);

            return Node::Branch(branch);
        }

        let child = Self::build_split(
            existing_hash,
            existing_address_indices,
            new_hash,
            new_address_index,
            level + 1,
        );

        Node::Branch(Branch::single(existing_index, Arc::new(child)))
    }
}

#[derive(Clone)]
enum Node {
    Leaf(u64, Vec<u32>),
    Branch(Branch),
}

#[derive(Clone)]
struct Branch {
    bitmap: u64,
    children: Vec<Arc<Node>>,
}

impl Branch {
    fn single(index: u32, child: Arc<Node>) -> Self {
        Self {
            bitmap: 1u64 << index,
            children: vec![child],
        }
    }

    fn pair(
        first_index: u32,
        first_child: Arc<Node>,
        second_index: u32,
        second_child: Arc<Node>,
    ) -> Self {
        let mut branch = Self::single(first_index, first_child);
        branch.insert_child(second_index, second_child);

        branch
    }

    const fn rank(&self, index: u32) -> usize {
        let bit = 1u64 << index;
        (self.bitmap & (bit - 1)).count_ones() as usize
    }

    fn child(&self, index: u32) -> Option<&Arc<Node>> {
        let bit = 1u64 << index;
        if self.bitmap & bit == 0 {
            None
        } else {
            Some(&self.children[self.rank(index)])
        }
    }

    fn insert_child(&mut self, index: u32, child: Arc<Node>) {
        let position = self.rank(index);
        self.children.insert(position, child);

        self.bitmap |= 1u64 << index;
    }

    fn remove_child(&mut self, index: u32) {
        let position = self.rank(index);
        self.children.remove(position);

        self.bitmap &= !(1u64 << index);
    }

    fn insert(&mut self, level: u32, hash: u64, address_index: u32) -> bool {
        let index = KeyDictionary::child_index(hash, level);

        if self.child(index).is_none() {
            self.insert_child(index, Arc::new(Node::Leaf(hash, vec![address_index])));
            return true;
        }

        let position = self.rank(index);

        KeyDictionary::insert_into_subtree(
            &mut self.children[position],
            level + 1,
            hash,
            address_index,
        )
    }

    fn remove(&mut self, level: u32, hash: u64, address_index: u32) -> (bool, bool) {
        let index = KeyDictionary::child_index(hash, level);

        if self.child(index).is_none() {
            return (false, false);
        }

        let position = self.rank(index);
        let (was_removed, child_now_empty) = KeyDictionary::remove_from_subtree(
            &mut self.children[position],
            level + 1,
            hash,
            address_index,
        );

        if child_now_empty {
            self.remove_child(index);
        }

        (was_removed, self.children.is_empty())
    }
}

#[cfg(test)]
mod test {
    use super::KeyDictionary;

    fn split_mix_64(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut result = *state;
        result = (result ^ (result >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        result = (result ^ (result >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);

        result ^ (result >> 31)
    }

    fn create_populated_dictionary(seed: u64, count: usize) -> (KeyDictionary, Vec<u64>, u64) {
        let mut dictionary = KeyDictionary::new();
        let mut state = seed;
        let hashes: Vec<_> = (0..count).map(|_| split_mix_64(&mut state)).collect();

        for (address_index, hash) in hashes.iter().enumerate() {
            dictionary.insert(*hash, address_index as u32);
        }

        let next_hash = split_mix_64(&mut state);

        (dictionary, hashes, next_hash)
    }

    #[test]
    fn test_new() {
        assert_eq!(0, KeyDictionary::new().candidates(0).count());
        assert_eq!(0, KeyDictionary::default().candidates(0).count());
    }

    #[test]
    fn test_hash_one() {
        let dictionary = KeyDictionary::new();

        assert_eq!(dictionary.hash_one("lorem"), dictionary.hash_one("lorem"));
    }

    #[test]
    fn test_candidates() {
        let mut dictionary = KeyDictionary::new();
        dictionary.insert(42, 7);

        assert_eq!(vec![7], dictionary.candidates(42).collect::<Vec<_>>());
        assert_eq!(0, dictionary.candidates(43).count());
    }

    #[test]
    fn test_insert() {
        let mut dictionary = KeyDictionary::new();
        assert!(dictionary.insert(42, 7));

        assert_eq!(vec![7], dictionary.candidates(42).collect::<Vec<_>>());

        assert!(dictionary.insert(42, 8));

        let mut candidates: Vec<_> = dictionary.candidates(42).collect();
        candidates.sort_unstable();
        assert_eq!(vec![7, 8], candidates);

        let (dictionary, hashes, _) = create_populated_dictionary(42, 10_000);

        for (address_index, hash) in hashes.iter().enumerate() {
            let candidates: Vec<_> = dictionary.candidates(*hash).collect();
            assert_eq!(vec![address_index as u32], candidates);
        }

        let (mut original, _, extra_hash) = create_populated_dictionary(7, 200);
        let clone = original.clone();
        assert!(original.insert(extra_hash, 999));

        assert_eq!(0, clone.candidates(extra_hash).count());
        assert_eq!(
            vec![999],
            original.candidates(extra_hash).collect::<Vec<_>>()
        );

        let mut dictionary = KeyDictionary::new();
        assert!(dictionary.insert(42, 7));
        assert!(!dictionary.insert(42, 7));

        assert_eq!(vec![7], dictionary.candidates(42).collect::<Vec<_>>());
    }

    #[test]
    fn test_remove() {
        let mut dictionary = KeyDictionary::new();
        dictionary.insert(42, 7);
        dictionary.insert(42, 8);

        assert!(dictionary.remove(42, 7));
        assert_eq!(vec![8], dictionary.candidates(42).collect::<Vec<_>>());

        assert!(dictionary.remove(42, 8));
        assert_eq!(0, dictionary.candidates(42).count());

        let (mut dictionary, hashes, _) = create_populated_dictionary(1, 500);

        for (address_index, hash) in hashes.iter().enumerate() {
            assert!(dictionary.remove(*hash, address_index as u32));
        }

        assert!(dictionary.root.is_none());

        let (mut original, hashes, _) = create_populated_dictionary(7, 200);
        let clone = original.clone();
        original.remove(hashes[0], 0);

        assert_eq!(vec![0], clone.candidates(hashes[0]).collect::<Vec<_>>());
        assert_eq!(0, original.candidates(hashes[0]).count());
    }

    #[test]
    fn test_invalid_remove() {
        let mut dictionary = KeyDictionary::new();

        assert!(!dictionary.remove(42, 7));

        dictionary.insert(42, 7);

        assert!(!dictionary.remove(42, 99));
        assert!(!dictionary.remove(43, 7));
        assert_eq!(vec![7], dictionary.candidates(42).collect::<Vec<_>>());
    }
}
