use super::presence::PresenceBitmap;
use crate::graphrecord::datatypes::{Identifier, IdentifierView};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct KeyChunk {
    present: PresenceBitmap,
    holds_string: PresenceBitmap,
    packed_keys: Vec<u64>,
    bytes: Vec<u8>,
}

impl KeyChunk {
    pub const fn new() -> Self {
        Self {
            present: PresenceBitmap::new(),
            holds_string: PresenceBitmap::new(),
            packed_keys: Vec::new(),
            bytes: Vec::new(),
        }
    }

    pub fn get(&self, cell_index: usize) -> Option<IdentifierView<'_>> {
        if !self.present.contains(cell_index) {
            return None;
        }

        let packed_key = self.packed_keys[self.present.rank(cell_index)];

        Some(self.view_of(cell_index, packed_key))
    }

    pub fn insert(&mut self, cell_index: usize, key: &Identifier) {
        if self.present.contains(cell_index) {
            return;
        }

        let rank = self.present.rank(cell_index);

        let packed_key = match key {
            Identifier::Int(value) => *value as u64,
            Identifier::String(value) => {
                let start = u32::try_from(self.bytes.len()).expect("Key bytes must fit in u32.");

                self.bytes.extend_from_slice(value.as_bytes());

                let end = u32::try_from(self.bytes.len()).expect("Key bytes must fit in u32.");

                self.holds_string.set(cell_index);

                Self::pack_string_span(start, end)
            }
        };

        self.packed_keys.insert(rank, packed_key);
        self.present.set(cell_index);
    }

    pub fn remove(&mut self, cell_index: usize) -> bool {
        if !self.present.contains(cell_index) {
            return false;
        }

        let rank = self.present.rank(cell_index);
        let is_string = self.holds_string.contains(cell_index);
        let removed_span = is_string.then(|| Self::unpack_string_span(self.packed_keys[rank]));

        self.packed_keys.remove(rank);
        self.present.clear(cell_index);
        self.holds_string.clear(cell_index);

        if let Some((removed_start, removed_end)) = removed_span {
            let removed_length = removed_end - removed_start;

            self.bytes.splice(
                removed_start as usize..removed_end as usize,
                std::iter::empty(),
            );

            for (packed_key_position, later_cell_index) in self.present.iter_present().enumerate() {
                if self.holds_string.contains(later_cell_index) {
                    let (start, end) =
                        Self::unpack_string_span(self.packed_keys[packed_key_position]);

                    if start > removed_start {
                        self.packed_keys[packed_key_position] =
                            Self::pack_string_span(start - removed_length, end - removed_length);
                    }
                }
            }
        }

        true
    }

    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, IdentifierView<'_>)> + '_ {
        self.present
            .iter_present()
            .zip(self.packed_keys.iter())
            .map(|(cell_index, &packed_key)| (cell_index, self.view_of(cell_index, packed_key)))
    }

    fn view_of(&self, cell_index: usize, packed_key: u64) -> IdentifierView<'_> {
        if self.holds_string.contains(cell_index) {
            let (start, end) = Self::unpack_string_span(packed_key);

            let text = std::str::from_utf8(&self.bytes[start as usize..end as usize])
                .expect("Key bytes must be valid UTF-8.");

            IdentifierView::String(Cow::Borrowed(text))
        } else {
            IdentifierView::Int(packed_key as i64)
        }
    }

    const fn pack_string_span(start: u32, end: u32) -> u64 {
        ((start as u64) << 32) | (end as u64)
    }

    const fn unpack_string_span(packed_key: u64) -> (u32, u32) {
        let start = (packed_key >> 32) as u32;
        let end = packed_key as u32;

        (start, end)
    }
}

impl Default for KeyChunk {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::KeyChunk;
    use crate::graphrecord::datatypes::{Identifier, IdentifierView};

    fn create_chunk_with_three_strings() -> KeyChunk {
        let mut chunk = KeyChunk::new();

        chunk.insert(0, &Identifier::String("lorem".to_string()));
        chunk.insert(1, &Identifier::String("do".to_string()));
        chunk.insert(2, &Identifier::String("consectetur".to_string()));

        chunk
    }

    #[test]
    fn test_new() {
        let chunk = KeyChunk::new();

        assert!(chunk.is_empty());
        assert_eq!(0, chunk.iter().count());
        assert_eq!(None, chunk.get(0));

        assert_eq!(0, KeyChunk::default().iter().count());
    }

    #[test]
    fn test_get() {
        let chunk = KeyChunk::new();

        assert_eq!(None, chunk.get(0));

        let mut chunk = KeyChunk::new();

        chunk.insert(3, &Identifier::Int(-42));

        assert_eq!(Some(IdentifierView::Int(-42)), chunk.get(3));

        let mut chunk = KeyChunk::new();

        chunk.insert(0, &Identifier::String("lorem".to_string()));

        assert_eq!(Some(IdentifierView::String("lorem".into())), chunk.get(0));

        let mut chunk = KeyChunk::new();

        chunk.insert(0, &Identifier::String(String::new()));
        chunk.insert(1, &Identifier::String("lorem".to_string()));

        assert_eq!(Some(IdentifierView::String("".into())), chunk.get(0));
        assert_eq!(Some(IdentifierView::String("lorem".into())), chunk.get(1));

        let mut chunk = KeyChunk::new();

        chunk.insert(0, &Identifier::Int(i64::MIN));
        chunk.insert(1, &Identifier::Int(i64::MAX));

        assert_eq!(Some(IdentifierView::Int(i64::MIN)), chunk.get(0));
        assert_eq!(Some(IdentifierView::Int(i64::MAX)), chunk.get(1));
    }

    #[test]
    fn test_insert() {
        let mut chunk = KeyChunk::new();

        chunk.insert(5, &Identifier::Int(1));
        chunk.insert(2, &Identifier::String("ipsum".to_string()));
        chunk.insert(9, &Identifier::Int(3));

        assert_eq!(Some(IdentifierView::Int(1)), chunk.get(5));
        assert_eq!(Some(IdentifierView::String("ipsum".into())), chunk.get(2));
        assert_eq!(Some(IdentifierView::Int(3)), chunk.get(9));
        assert_eq!(3, chunk.iter().count());
    }

    #[test]
    fn test_remove() {
        let mut chunk = KeyChunk::new();

        chunk.insert(3, &Identifier::Int(-42));

        assert!(chunk.remove(3));

        assert_eq!(None, chunk.get(3));
        assert!(chunk.is_empty());
        assert!(!chunk.remove(3));

        let mut chunk = KeyChunk::new();

        chunk.insert(0, &Identifier::String("lorem".to_string()));

        assert!(chunk.remove(0));

        assert_eq!(None, chunk.get(0));

        let mut chunk = create_chunk_with_three_strings();

        assert!(chunk.remove(1));

        assert_eq!(Some(IdentifierView::String("lorem".into())), chunk.get(0));
        assert_eq!(None, chunk.get(1));
        assert_eq!(
            Some(IdentifierView::String("consectetur".into())),
            chunk.get(2)
        );

        let mut chunk = KeyChunk::new();

        chunk.insert(0, &Identifier::String("lorem".to_string()));
        chunk.insert(1, &Identifier::Int(7));
        chunk.insert(2, &Identifier::String("consectetur".to_string()));

        assert!(chunk.remove(0));

        assert_eq!(Some(IdentifierView::Int(7)), chunk.get(1));
        assert_eq!(
            Some(IdentifierView::String("consectetur".into())),
            chunk.get(2)
        );

        let mut chunk = KeyChunk::new();

        chunk.insert(0, &Identifier::String(String::new()));
        chunk.insert(1, &Identifier::String("lorem".to_string()));

        assert!(chunk.remove(0));

        assert_eq!(None, chunk.get(0));
        assert_eq!(Some(IdentifierView::String("lorem".into())), chunk.get(1));
    }

    #[test]
    fn test_is_empty() {
        let mut chunk = KeyChunk::new();

        assert!(chunk.is_empty());

        chunk.insert(3, &Identifier::Int(-42));

        assert!(!chunk.is_empty());

        assert!(chunk.remove(3));

        assert!(chunk.is_empty());
    }

    #[test]
    fn test_iter() {
        let mut chunk = KeyChunk::new();

        chunk.insert(7, &Identifier::Int(2));
        chunk.insert(1, &Identifier::String("lorem".to_string()));

        let collected: Vec<_> = chunk.iter().collect();

        assert_eq!(
            vec![
                (1, IdentifierView::String("lorem".into())),
                (7, IdentifierView::Int(2)),
            ],
            collected
        );
    }
}
