use super::{CHUNK_TREE_BITS_PER_LEVEL, CHUNK_TREE_CHILDREN_PER_BRANCH};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct ChunkTree<C> {
    root: Option<Node<C>>,
    height: u8,
}

impl<C> ChunkTree<C> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            root: None,
            height: 0,
        }
    }

    #[must_use]
    pub fn get(&self, chunk_index: u32) -> Option<&C> {
        if Self::required_height(chunk_index) > self.height {
            return None;
        }

        let mut node = self.root.as_ref()?;
        let mut remaining_height = self.height;

        while remaining_height > 0 {
            let Node::Branch(branch) = node else {
                return None;
            };

            node = branch.children[Self::child_index(chunk_index, remaining_height)].as_ref()?;
            remaining_height -= 1;
        }

        match node {
            Node::Chunk(chunk) => Some(chunk),
            Node::Branch(_) => unreachable!("Chunk tree descent must terminate at a chunk."),
        }
    }

    pub fn chunks(&self) -> impl Iterator<Item = (u32, &C)> {
        Chunks::new(self)
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    #[allow(dead_code)]
    #[must_use]
    pub const fn cursor(&self) -> ChunkTreeCursor<'_, C> {
        ChunkTreeCursor {
            chunk_tree: self,
            cached: None,
        }
    }

    pub fn remove_chunk(&mut self, chunk_index: u32) -> bool {
        if self.get(chunk_index).is_none() {
            return false;
        }

        Self::remove_recursive(&mut self.root, self.height, chunk_index);
        self.shrink();

        true
    }

    fn remove_recursive(node: &mut Option<Node<C>>, remaining_height: u8, chunk_index: u32) {
        if node.is_none() {
            return;
        }

        if remaining_height == 0 {
            *node = None;
            return;
        }

        let Some(Node::Branch(branch)) = node else {
            unreachable!("Nodes above the leaf level must be branches.");
        };

        let branch = Arc::make_mut(branch);
        Self::remove_recursive(
            &mut branch.children[Self::child_index(chunk_index, remaining_height)],
            remaining_height - 1,
            chunk_index,
        );

        if branch.children.iter().all(Option::is_none) {
            *node = None;
        }
    }

    fn shrink(&mut self) {
        loop {
            match self.root.as_ref() {
                None => {
                    self.height = 0;
                    return;
                }
                Some(Node::Chunk(_)) => {
                    return;
                }
                Some(Node::Branch(branch)) => {
                    if branch.children[1..].iter().any(Option::is_some) {
                        return;
                    }

                    self.root = branch.children[0].clone();
                    self.height -= 1;
                }
            }
        }
    }

    fn required_height(chunk_index: u32) -> u8 {
        let mut height: u8 = 0;
        let mut capacity: u64 = 1;

        while u64::from(chunk_index) >= capacity {
            capacity *= CHUNK_TREE_CHILDREN_PER_BRANCH as u64;
            height += 1;
        }

        height
    }

    fn child_index(chunk_index: u32, remaining_height: u8) -> usize {
        let shift = CHUNK_TREE_BITS_PER_LEVEL * (u32::from(remaining_height) - 1);
        ((chunk_index >> shift) & (CHUNK_TREE_CHILDREN_PER_BRANCH as u32 - 1)) as usize
    }
}

impl<C: Clone> ChunkTree<C> {
    #[must_use]
    pub fn get_mut(&mut self, chunk_index: u32) -> Option<&mut C> {
        self.get(chunk_index)?;

        let mut node = self.root.as_mut()?;
        let mut remaining_height = self.height;

        while remaining_height > 0 {
            let Node::Branch(branch) = node else {
                return None;
            };

            node = Arc::make_mut(branch).children[Self::child_index(chunk_index, remaining_height)]
                .as_mut()?;
            remaining_height -= 1;
        }

        match node {
            Node::Chunk(chunk) => Some(Arc::make_mut(chunk)),
            Node::Branch(_) => unreachable!("Chunk tree descent must terminate at a chunk."),
        }
    }

    pub fn get_mut_or_insert_with(
        &mut self,
        chunk_index: u32,
        create: impl FnOnce() -> C,
    ) -> &mut C {
        let target_height = Self::required_height(chunk_index).max(self.height);

        if self.root.is_some() {
            while self.height < target_height {
                let existing = self.root.take();
                let mut children: [Option<Node<C>>; CHUNK_TREE_CHILDREN_PER_BRANCH] =
                    std::array::from_fn(|_| None);
                children[0] = existing;

                self.root = Some(Node::Branch(Arc::new(Branch { children })));
                self.height += 1;
            }
        } else {
            self.height = target_height;
        }

        let height = self.height;
        let mut create = Some(create);
        let mut node = self
            .root
            .get_or_insert_with(|| Self::node_for(height, &mut create));
        let mut remaining_height = height;

        while remaining_height > 0 {
            let Node::Branch(branch) = node else {
                unreachable!("Chunk tree descent must not reach a chunk early.");
            };

            let branch = Arc::make_mut(branch);
            let child_index = Self::child_index(chunk_index, remaining_height);
            let child_remaining_height = remaining_height - 1;

            node = branch.children[child_index]
                .get_or_insert_with(|| Self::node_for(child_remaining_height, &mut create));
            remaining_height -= 1;
        }

        let Node::Chunk(chunk) = node else {
            unreachable!("Chunk tree descent must terminate at a chunk.");
        };

        Arc::make_mut(chunk)
    }

    fn node_for(remaining_height: u8, create: &mut Option<impl FnOnce() -> C>) -> Node<C> {
        if remaining_height > 0 {
            return Node::Branch(Arc::new(Branch {
                children: std::array::from_fn(|_| None),
            }));
        }

        let create = create
            .take()
            .expect("The leaf must be created exactly once.");

        Node::Chunk(Arc::new(create()))
    }
}

impl<C: Clone + Default> ChunkTree<C> {
    pub fn get_mut_or_default(&mut self, chunk_index: u32) -> &mut C {
        self.get_mut_or_insert_with(chunk_index, C::default)
    }
}

impl<C> Clone for ChunkTree<C> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            height: self.height,
        }
    }
}

impl<C> Default for ChunkTree<C> {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
pub struct ChunkTreeCursor<'a, C> {
    chunk_tree: &'a ChunkTree<C>,
    cached: Option<(u32, Option<&'a C>)>,
}

impl<'a, C> ChunkTreeCursor<'a, C> {
    #[allow(dead_code)]
    pub fn get(&mut self, chunk_index: u32) -> Option<&'a C> {
        if let Some((cached_chunk_index, cached_chunk)) = self.cached
            && cached_chunk_index == chunk_index
        {
            return cached_chunk;
        }

        let chunk = self.chunk_tree.get(chunk_index);
        self.cached = Some((chunk_index, chunk));

        chunk
    }
}

#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
enum Node<C> {
    Branch(Arc<Branch<C>>),
    Chunk(Arc<C>),
}

impl<C> Clone for Node<C> {
    fn clone(&self) -> Self {
        match self {
            Self::Branch(branch) => Self::Branch(Arc::clone(branch)),
            Self::Chunk(chunk) => Self::Chunk(Arc::clone(chunk)),
        }
    }
}

#[cfg(any(feature = "serde", feature = "io"))]
mod children_serde {
    use super::{CHUNK_TREE_CHILDREN_PER_BRANCH, Node};
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

    pub(super) fn serialize<C: Serialize, S: Serializer>(
        children: &[Option<Node<C>>; CHUNK_TREE_CHILDREN_PER_BRANCH],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        children.as_slice().serialize(serializer)
    }

    pub(super) fn deserialize<'de, C: Deserialize<'de>, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<[Option<Node<C>>; CHUNK_TREE_CHILDREN_PER_BRANCH], D::Error> {
        let children = Vec::<Option<Node<C>>>::deserialize(deserializer)?;
        let length = children.len();

        children
            .try_into()
            .map_err(|_| D::Error::invalid_length(length, &"a branch's full set of children"))
    }
}

#[cfg_attr(
    any(feature = "serde", feature = "io"),
    derive(Serialize, Deserialize),
    serde(bound(serialize = "C: Serialize", deserialize = "C: Deserialize<'de>"))
)]
struct Branch<C> {
    #[cfg_attr(any(feature = "serde", feature = "io"), serde(with = "children_serde"))]
    children: [Option<Node<C>>; CHUNK_TREE_CHILDREN_PER_BRANCH],
}

impl<C> Clone for Branch<C> {
    fn clone(&self) -> Self {
        Self {
            children: self.children.clone(),
        }
    }
}

struct Chunks<'a, C> {
    stack: Vec<ChunksFrame<'a, C>>,
    root_chunk: Option<(u32, &'a C)>,
}

struct ChunksFrame<'a, C> {
    branch: &'a Branch<C>,
    base_chunk_index: u32,
    shift: u32,
    next_child_index: usize,
}

impl<'a, C> Chunks<'a, C> {
    fn new(chunk_tree: &'a ChunkTree<C>) -> Self {
        let mut stack = Vec::with_capacity(chunk_tree.height as usize);
        let mut root_chunk: Option<(u32, &'a C)> = None;

        match chunk_tree.root.as_ref() {
            Some(Node::Chunk(chunk)) => root_chunk = Some((0, chunk)),
            Some(Node::Branch(branch)) => stack.push(ChunksFrame {
                branch,
                base_chunk_index: 0,
                shift: CHUNK_TREE_BITS_PER_LEVEL * u32::from(chunk_tree.height - 1),
                next_child_index: 0,
            }),
            None => {}
        }

        Self { stack, root_chunk }
    }
}

impl<'a, C> Iterator for Chunks<'a, C> {
    type Item = (u32, &'a C);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(root_chunk) = self.root_chunk.take() {
            return Some(root_chunk);
        }

        loop {
            let frame = self.stack.last_mut()?;

            if frame.next_child_index == CHUNK_TREE_CHILDREN_PER_BRANCH {
                self.stack.pop();
                continue;
            }

            let child_index = frame.next_child_index;
            frame.next_child_index += 1;

            let branch = frame.branch;
            let base_chunk_index = frame.base_chunk_index;
            let shift = frame.shift;

            let Some(child) = &branch.children[child_index] else {
                continue;
            };

            let child_base_chunk_index = base_chunk_index | ((child_index as u32) << shift);

            match child {
                Node::Chunk(chunk) => return Some((child_base_chunk_index, chunk)),
                Node::Branch(child_branch) => {
                    self.stack.push(ChunksFrame {
                        branch: child_branch,
                        base_chunk_index: child_base_chunk_index,
                        shift: shift - CHUNK_TREE_BITS_PER_LEVEL,
                        next_child_index: 0,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::ChunkTree;

    #[test]
    fn test_chunk_tree_new() {
        let chunk_tree: ChunkTree<u32> = ChunkTree::new();

        assert!(chunk_tree.get(0).is_none());
        assert_eq!(0, chunk_tree.chunks().count());

        let default_chunk_tree: ChunkTree<u32> = ChunkTree::default();

        assert!(default_chunk_tree.get(0).is_none());
    }

    #[test]
    fn test_chunk_tree_get() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 7;

        assert_eq!(Some(&7), chunk_tree.get(0));
        assert!(chunk_tree.get(1).is_none());
    }

    #[test]
    fn test_chunk_tree_chunks() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();

        for chunk_index in [4096, 0, 64, 1, 4095, 63] {
            *chunk_tree.get_mut_or_default(chunk_index) = chunk_index;
        }

        let observed: Vec<_> = chunk_tree
            .chunks()
            .map(|(chunk_index, _)| chunk_index)
            .collect();

        assert_eq!(vec![0, 1, 63, 64, 4095, 4096], observed);

        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 10;
        *chunk_tree.get_mut_or_default(64) = 20;

        let observed: Vec<_> = chunk_tree
            .chunks()
            .map(|(chunk_index, chunk)| (chunk_index, *chunk))
            .collect();

        assert_eq!(vec![(0, 10), (64, 20)], observed);
    }

    #[test]
    fn test_chunk_tree_is_empty() {
        let chunk_tree: ChunkTree<u32> = ChunkTree::new();

        assert!(chunk_tree.is_empty());

        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 1;

        assert!(!chunk_tree.is_empty());
    }

    #[test]
    fn test_chunk_tree_cursor() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 10;

        let mut cursor = chunk_tree.cursor();

        assert_eq!(Some(&10), cursor.get(0));
    }

    #[test]
    fn test_chunk_tree_remove_chunk() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 1;
        *chunk_tree.get_mut_or_default(1) = 2;

        assert!(chunk_tree.remove_chunk(0));

        assert!(chunk_tree.get(0).is_none());
        assert_eq!(Some(&2), chunk_tree.get(1));

        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(4096) = 1;

        assert!(chunk_tree.remove_chunk(4096));

        assert!(chunk_tree.get(4096).is_none());
        assert_eq!(0, chunk_tree.chunks().count());

        *chunk_tree.get_mut_or_default(0) = 42;
        assert_eq!(Some(&42), chunk_tree.get(0));

        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 1;
        *chunk_tree.get_mut_or_default(4096) = 2;

        assert!(chunk_tree.remove_chunk(4096));

        assert_eq!(Some(&1), chunk_tree.get(0));
        assert!(chunk_tree.get(4096).is_none());

        *chunk_tree.get_mut_or_default(64) = 3;
        assert_eq!(Some(&1), chunk_tree.get(0));
        assert_eq!(Some(&3), chunk_tree.get(64));
    }

    #[test]
    fn test_invalid_chunk_tree_remove_chunk() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        assert!(!chunk_tree.remove_chunk(0));

        assert!(chunk_tree.get(0).is_none());

        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 1;

        assert!(!chunk_tree.remove_chunk(4096));

        assert_eq!(Some(&1), chunk_tree.get(0));

        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 1;
        *chunk_tree.get_mut_or_default(4096) = 2;

        let mut cloned = chunk_tree.clone();
        assert!(!cloned.remove_chunk(1));

        assert_eq!(Some(&1), cloned.get(0));

        let original_pointer = std::ptr::from_ref(chunk_tree.get(0).unwrap());
        let cloned_pointer = std::ptr::from_ref(cloned.get(0).unwrap());
        assert_eq!(cloned_pointer, original_pointer);
    }

    #[test]
    fn test_chunk_tree_get_mut() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(4096) = 7;

        *chunk_tree.get_mut(4096).unwrap() = 8;

        assert_eq!(Some(&8), chunk_tree.get(4096));
        assert!(chunk_tree.get_mut(0).is_none());
        assert!(chunk_tree.get_mut(4097).is_none());

        let shared = chunk_tree.clone();

        *chunk_tree.get_mut(4096).unwrap() = 9;

        assert_eq!(Some(&9), chunk_tree.get(4096));
        assert_eq!(Some(&8), shared.get(4096));
    }

    #[test]
    fn test_chunk_tree_get_mut_or_insert_with() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();

        for chunk_index in [0, 1, 63, 64, 4095, 4096] {
            *chunk_tree.get_mut_or_insert_with(chunk_index, u32::default) = chunk_index;
        }

        for chunk_index in [0, 1, 63, 64, 4095, 4096] {
            assert_eq!(Some(&chunk_index), chunk_tree.get(chunk_index));
        }

        assert!(chunk_tree.get(4097).is_none());

        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_insert_with(5, u32::default) = 55;
        *chunk_tree.get_mut_or_insert_with(5000, u32::default) = 5555;

        assert_eq!(Some(&55), chunk_tree.get(5));
        assert_eq!(Some(&5555), chunk_tree.get(5000));
    }

    #[test]
    fn test_chunk_tree_get_mut_or_default() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();

        assert_eq!(0, *chunk_tree.get_mut_or_default(64));
    }

    #[test]
    fn test_chunk_tree_clone() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 1;
        *chunk_tree.get_mut_or_default(4096) = 2;

        let cloned = chunk_tree.clone();

        assert_eq!(chunk_tree.get(0), cloned.get(0));
        assert_eq!(chunk_tree.get(4096), cloned.get(4096));

        let original_pointer = std::ptr::from_ref(chunk_tree.get(0).unwrap());
        let cloned_pointer = std::ptr::from_ref(cloned.get(0).unwrap());
        assert_eq!(cloned_pointer, original_pointer);

        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 1;

        let mut cloned = chunk_tree.clone();
        *cloned.get_mut_or_default(0) = 99;

        assert_eq!(Some(&1), chunk_tree.get(0));
        assert_eq!(Some(&99), cloned.get(0));

        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 1;
        *chunk_tree.get_mut_or_default(4096) = 2;

        let mut cloned = chunk_tree.clone();
        *cloned.get_mut_or_default(0) = 99;

        assert_eq!(Some(&2), chunk_tree.get(4096));
        assert_eq!(Some(&2), cloned.get(4096));

        let original_pointer = std::ptr::from_ref(chunk_tree.get(4096).unwrap());
        let cloned_pointer = std::ptr::from_ref(cloned.get(4096).unwrap());
        assert_eq!(cloned_pointer, original_pointer);
    }

    #[test]
    fn test_chunk_tree_cursor_get() {
        let mut chunk_tree: ChunkTree<u32> = ChunkTree::new();
        *chunk_tree.get_mut_or_default(0) = 10;
        *chunk_tree.get_mut_or_default(4096) = 20;

        let mut cursor = chunk_tree.cursor();

        assert_eq!(Some(&10), cursor.get(0));
        assert_eq!(Some(&10), cursor.get(0));
        assert_eq!(Some(&20), cursor.get(4096));
        assert_eq!(Some(&20), cursor.get(4096));
        assert_eq!(None, cursor.get(1));
        assert_eq!(None, cursor.get(1));
        assert_eq!(Some(&10), cursor.get(0));
    }
}
