use super::CHUNK_BITMAP_WORDS;
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct PresenceBitmap([u64; CHUNK_BITMAP_WORDS]);

impl PresenceBitmap {
    #[must_use]
    pub const fn new() -> Self {
        Self([0; CHUNK_BITMAP_WORDS])
    }

    #[must_use]
    pub const fn contains(&self, cell_index: usize) -> bool {
        let (word_index, bit_index) = Self::locate(cell_index);

        self.0[word_index] & (1 << bit_index) != 0
    }

    #[must_use]
    pub fn rank(&self, cell_index: usize) -> usize {
        let (word_index, bit_index) = Self::locate(cell_index);

        let set_bits_in_words_below: usize = self.0[..word_index]
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum();
        let set_bits_in_word_below_bit =
            (self.0[word_index] & ((1u64 << bit_index) - 1)).count_ones() as usize;

        set_bits_in_words_below + set_bits_in_word_below_bit
    }

    pub fn set(&mut self, cell_index: usize) -> usize {
        let rank = self.rank(cell_index);

        let (word_index, bit_index) = Self::locate(cell_index);
        self.0[word_index] |= 1 << bit_index;

        rank
    }

    pub fn clear(&mut self, cell_index: usize) -> usize {
        let rank = self.rank(cell_index);

        let (word_index, bit_index) = Self::locate(cell_index);
        self.0[word_index] &= !(1 << bit_index);

        rank
    }

    pub fn iter_present(&self) -> impl Iterator<Item = usize> + '_ {
        self.0
            .iter()
            .enumerate()
            .flat_map(|(word_index, &word)| PresentBits {
                word,
                base_cell_index: word_index * 64,
            })
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|word| *word == 0)
    }

    const fn locate(cell_index: usize) -> (usize, u64) {
        (cell_index / 64, (cell_index % 64) as u64)
    }
}

impl Default for PresenceBitmap {
    fn default() -> Self {
        Self::new()
    }
}

struct PresentBits {
    word: u64,
    base_cell_index: usize,
}

impl Iterator for PresentBits {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.word == 0 {
            return None;
        }

        let bit_index = self.word.trailing_zeros() as usize;
        self.word &= !(1 << bit_index);

        Some(self.base_cell_index + bit_index)
    }
}

#[cfg(test)]
mod test {
    use super::PresenceBitmap;

    fn create_bitmap() -> PresenceBitmap {
        let mut bitmap = PresenceBitmap::new();
        bitmap.set(0);
        bitmap.set(63);
        bitmap.set(64);
        bitmap.set(255);

        bitmap
    }

    #[test]
    fn test_new() {
        assert!(PresenceBitmap::new().is_empty());
        assert_eq!(0, PresenceBitmap::new().iter_present().count());
        assert!(PresenceBitmap::default().is_empty());
    }

    #[test]
    fn test_contains() {
        let bitmap = create_bitmap();

        assert!(bitmap.contains(0));
        assert!(bitmap.contains(63));
        assert!(bitmap.contains(64));
        assert!(bitmap.contains(255));
        assert!(!bitmap.contains(1));
    }

    #[test]
    fn test_rank() {
        let bitmap = create_bitmap();

        assert_eq!(0, bitmap.rank(0));
        assert_eq!(1, bitmap.rank(1));
        assert_eq!(1, bitmap.rank(63));
        assert_eq!(2, bitmap.rank(64));
        assert_eq!(3, bitmap.rank(65));
        assert_eq!(3, bitmap.rank(255));
    }

    #[test]
    fn test_set() {
        let mut bitmap = PresenceBitmap::new();

        assert_eq!(0, bitmap.set(64));
        assert_eq!(0, bitmap.set(0));
        assert_eq!(2, bitmap.set(255));
        assert_eq!(1, bitmap.set(63));
        assert_eq!(4, bitmap.iter_present().count());

        assert_eq!(2, bitmap.set(64));
        assert_eq!(4, bitmap.iter_present().count());
    }

    #[test]
    fn test_clear() {
        let mut bitmap = PresenceBitmap::new();
        bitmap.set(0);
        bitmap.set(64);
        bitmap.set(128);

        assert_eq!(1, bitmap.clear(64));
        assert!(!bitmap.contains(64));
        assert_eq!(2, bitmap.iter_present().count());
        assert_eq!(1, bitmap.rank(128));

        assert_eq!(1, bitmap.clear(64));
        assert_eq!(2, bitmap.iter_present().count());
        assert!(!bitmap.contains(64));
    }

    #[test]
    fn test_iter_present() {
        let bitmap = PresenceBitmap::new();

        assert_eq!(
            Vec::<usize>::new(),
            bitmap.iter_present().collect::<Vec<_>>()
        );

        let mut bitmap = PresenceBitmap::new();
        for cell_index in [255, 128, 0, 64, 63, 65] {
            bitmap.set(cell_index);
        }

        assert_eq!(
            vec![0, 63, 64, 65, 128, 255],
            bitmap.iter_present().collect::<Vec<_>>()
        );

        let mut bitmap = PresenceBitmap::new();
        for cell_index in 0..256 {
            bitmap.set(cell_index);
        }

        assert_eq!(
            (0..256).collect::<Vec<_>>(),
            bitmap.iter_present().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_is_empty() {
        let mut bitmap = PresenceBitmap::new();

        assert!(bitmap.is_empty());

        bitmap.set(64);

        assert!(!bitmap.is_empty());

        bitmap.clear(64);

        assert!(bitmap.is_empty());
    }
}
