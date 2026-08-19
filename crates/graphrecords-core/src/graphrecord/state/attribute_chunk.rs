use super::presence::PresenceBitmap;
use crate::graphrecord::datatypes::{Value, ValueView};
use chrono::{NaiveDateTime, TimeDelta};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub enum AttributeChunk {
    Int(ScalarCells<i64>),
    Float(ScalarCells<f64>),
    Bool(BoolCells),
    DateTime(ScalarCells<NaiveDateTime>),
    Duration(ScalarCells<TimeDelta>),
    String(StringCells),
    Mixed(ScalarCells<Value>),
}

impl AttributeChunk {
    pub const fn new(value: &Value) -> Self {
        match value {
            Value::Int(_) => Self::Int(ScalarCells::new()),
            Value::Float(_) => Self::Float(ScalarCells::new()),
            Value::Bool(_) => Self::Bool(BoolCells::new()),
            Value::DateTime(_) => Self::DateTime(ScalarCells::new()),
            Value::Duration(_) => Self::Duration(ScalarCells::new()),
            Value::String(_) => Self::String(StringCells::new()),
            Value::Null => Self::Mixed(ScalarCells::new()),
        }
    }

    pub fn get(&self, cell_index: usize) -> Option<ValueView<'_>> {
        match self {
            Self::Int(scalar_cells) => match scalar_cells.get(cell_index) {
                Cell::Absent => None,
                Cell::Null => Some(ValueView::Null),
                Cell::Value(value) => Some(ValueView::Int(*value)),
            },
            Self::Float(scalar_cells) => match scalar_cells.get(cell_index) {
                Cell::Absent => None,
                Cell::Null => Some(ValueView::Null),
                Cell::Value(value) => Some(ValueView::Float(*value)),
            },
            Self::Bool(bool_cells) => match bool_cells.get(cell_index) {
                Cell::Absent => None,
                Cell::Null => Some(ValueView::Null),
                Cell::Value(value) => Some(ValueView::Bool(value)),
            },
            Self::DateTime(scalar_cells) => match scalar_cells.get(cell_index) {
                Cell::Absent => None,
                Cell::Null => Some(ValueView::Null),
                Cell::Value(value) => Some(ValueView::DateTime(*value)),
            },
            Self::Duration(scalar_cells) => match scalar_cells.get(cell_index) {
                Cell::Absent => None,
                Cell::Null => Some(ValueView::Null),
                Cell::Value(value) => Some(ValueView::Duration(*value)),
            },
            Self::String(string_cells) => match string_cells.get(cell_index) {
                Cell::Absent => None,
                Cell::Null => Some(ValueView::Null),
                Cell::Value(value) => Some(ValueView::String(Cow::Borrowed(value))),
            },
            Self::Mixed(scalar_cells) => match scalar_cells.get(cell_index) {
                Cell::Absent => None,
                Cell::Null => Some(ValueView::Null),
                Cell::Value(value) => Some(ValueView::from(value)),
            },
        }
    }

    pub fn set(&mut self, cell_index: usize, value: &Value) {
        if matches!(value, Value::Null) {
            match self {
                Self::Int(scalar_cells) => scalar_cells.set_null(cell_index),
                Self::Float(scalar_cells) => scalar_cells.set_null(cell_index),
                Self::Bool(bool_cells) => bool_cells.set_null(cell_index),
                Self::DateTime(scalar_cells) => scalar_cells.set_null(cell_index),
                Self::Duration(scalar_cells) => scalar_cells.set_null(cell_index),
                Self::String(string_cells) => string_cells.set_null(cell_index),
                Self::Mixed(scalar_cells) => scalar_cells.set_null(cell_index),
            }

            return;
        }

        if !self.matches_variant(value) {
            self.convert_to_mixed();
        }

        match (self, value) {
            (Self::Int(scalar_cells), Value::Int(inner)) => scalar_cells.set(cell_index, *inner),
            (Self::Float(scalar_cells), Value::Float(inner)) => {
                scalar_cells.set(cell_index, *inner);
            }
            (Self::Bool(bool_cells), Value::Bool(inner)) => bool_cells.set(cell_index, *inner),
            (Self::DateTime(scalar_cells), Value::DateTime(inner)) => {
                scalar_cells.set(cell_index, *inner);
            }
            (Self::Duration(scalar_cells), Value::Duration(inner)) => {
                scalar_cells.set(cell_index, *inner);
            }
            (Self::String(string_cells), Value::String(inner)) => {
                string_cells.set(cell_index, inner);
            }
            (Self::Mixed(scalar_cells), _) => scalar_cells.set(cell_index, value.clone()),
            _ => unreachable!("Chunk variant must match the value or be Mixed."),
        }
    }

    pub fn remove(&mut self, cell_index: usize) -> bool {
        match self {
            Self::Int(scalar_cells) => scalar_cells.remove(cell_index),
            Self::Float(scalar_cells) => scalar_cells.remove(cell_index),
            Self::Bool(bool_cells) => bool_cells.remove(cell_index),
            Self::DateTime(scalar_cells) => scalar_cells.remove(cell_index),
            Self::Duration(scalar_cells) => scalar_cells.remove(cell_index),
            Self::String(string_cells) => string_cells.remove(cell_index),
            Self::Mixed(scalar_cells) => scalar_cells.remove(cell_index),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Int(scalar_cells) => scalar_cells.is_empty(),
            Self::Float(scalar_cells) => scalar_cells.is_empty(),
            Self::Bool(bool_cells) => bool_cells.is_empty(),
            Self::DateTime(scalar_cells) => scalar_cells.is_empty(),
            Self::Duration(scalar_cells) => scalar_cells.is_empty(),
            Self::String(string_cells) => string_cells.is_empty(),
            Self::Mixed(scalar_cells) => scalar_cells.is_empty(),
        }
    }

    const fn matches_variant(&self, value: &Value) -> bool {
        matches!(
            (self, value),
            (Self::Int(_), Value::Int(_))
                | (Self::Float(_), Value::Float(_))
                | (Self::Bool(_), Value::Bool(_))
                | (Self::DateTime(_), Value::DateTime(_))
                | (Self::Duration(_), Value::Duration(_))
                | (Self::String(_), Value::String(_))
                | (Self::Mixed(_), _)
        )
    }

    fn convert_to_mixed(&mut self) {
        *self = match self {
            Self::Mixed(_) => return,
            Self::Int(scalar_cells) => {
                Self::rebuild_mixed(scalar_cells, |value| Value::Int(*value))
            }
            Self::Float(scalar_cells) => {
                Self::rebuild_mixed(scalar_cells, |value| Value::Float(*value))
            }
            Self::Bool(bool_cells) => Self::rebuild_mixed_from_bool(bool_cells),
            Self::DateTime(scalar_cells) => {
                Self::rebuild_mixed(scalar_cells, |value| Value::DateTime(*value))
            }
            Self::Duration(scalar_cells) => {
                Self::rebuild_mixed(scalar_cells, |value| Value::Duration(*value))
            }
            Self::String(string_cells) => Self::rebuild_mixed_from_string(string_cells),
        };
    }

    fn rebuild_mixed<T>(scalar_cells: &ScalarCells<T>, to_value: impl Fn(&T) -> Value) -> Self {
        Self::Mixed(ScalarCells {
            present: scalar_cells.present.clone(),
            null: scalar_cells.null.clone(),
            cells: scalar_cells.cells.iter().map(to_value).collect(),
        })
    }

    fn rebuild_mixed_from_bool(bool_cells: &BoolCells) -> Self {
        let cells = bool_cells
            .present
            .iter_present()
            .filter(|&cell_index| !bool_cells.null.contains(cell_index))
            .map(|cell_index| Value::Bool(bool_cells.truth.contains(cell_index)))
            .collect();

        Self::Mixed(ScalarCells {
            present: bool_cells.present.clone(),
            null: bool_cells.null.clone(),
            cells,
        })
    }

    fn rebuild_mixed_from_string(string_cells: &StringCells) -> Self {
        let cells = string_cells
            .iter()
            .filter_map(|(_, value)| value.map(|value| Value::String(value.to_string())))
            .collect();

        Self::Mixed(ScalarCells {
            present: string_cells.present.clone(),
            null: string_cells.null.clone(),
            cells,
        })
    }
}

enum Cell<T> {
    Absent,
    Null,
    Value(T),
}

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct ScalarCells<T> {
    present: PresenceBitmap,
    null: PresenceBitmap,
    cells: Vec<T>,
}

impl<T> ScalarCells<T> {
    const fn new() -> Self {
        Self {
            present: PresenceBitmap::new(),
            null: PresenceBitmap::new(),
            cells: Vec::new(),
        }
    }

    fn get(&self, cell_index: usize) -> Cell<&T> {
        if !self.present.contains(cell_index) {
            return Cell::Absent;
        }

        if self.null.contains(cell_index) {
            return Cell::Null;
        }

        Cell::Value(&self.cells[self.dense_rank(cell_index)])
    }

    fn set(&mut self, cell_index: usize, value: T) {
        let was_present = self.present.contains(cell_index);
        let was_null = was_present && self.null.contains(cell_index);
        let rank = self.dense_rank(cell_index);

        self.present.set(cell_index);
        self.null.clear(cell_index);

        if was_present && !was_null {
            self.cells[rank] = value;
        } else {
            self.cells.insert(rank, value);
        }
    }

    fn set_null(&mut self, cell_index: usize) {
        let was_present = self.present.contains(cell_index);
        let was_null = was_present && self.null.contains(cell_index);

        if was_present && !was_null {
            let rank = self.dense_rank(cell_index);

            self.cells.remove(rank);
        }

        self.present.set(cell_index);
        self.null.set(cell_index);
    }

    fn remove(&mut self, cell_index: usize) -> bool {
        if !self.present.contains(cell_index) {
            return false;
        }

        if !self.null.contains(cell_index) {
            let rank = self.dense_rank(cell_index);

            self.cells.remove(rank);
        }

        self.present.clear(cell_index);
        self.null.clear(cell_index);

        true
    }

    fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    fn dense_rank(&self, cell_index: usize) -> usize {
        self.present.rank(cell_index) - self.null.rank(cell_index)
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct BoolCells {
    present: PresenceBitmap,
    null: PresenceBitmap,
    truth: PresenceBitmap,
}

impl BoolCells {
    const fn new() -> Self {
        Self {
            present: PresenceBitmap::new(),
            null: PresenceBitmap::new(),
            truth: PresenceBitmap::new(),
        }
    }

    const fn get(&self, cell_index: usize) -> Cell<bool> {
        if !self.present.contains(cell_index) {
            return Cell::Absent;
        }

        if self.null.contains(cell_index) {
            return Cell::Null;
        }

        Cell::Value(self.truth.contains(cell_index))
    }

    fn set(&mut self, cell_index: usize, value: bool) {
        self.present.set(cell_index);
        self.null.clear(cell_index);

        if value {
            self.truth.set(cell_index);
        } else {
            self.truth.clear(cell_index);
        }
    }

    fn set_null(&mut self, cell_index: usize) {
        self.present.set(cell_index);
        self.null.set(cell_index);
    }

    fn remove(&mut self, cell_index: usize) -> bool {
        if !self.present.contains(cell_index) {
            return false;
        }

        self.present.clear(cell_index);
        self.null.clear(cell_index);
        self.truth.clear(cell_index);

        true
    }

    fn is_empty(&self) -> bool {
        self.present.is_empty()
    }
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
struct StringSpan {
    start: u32,
    end: u32,
}

#[derive(Debug, Clone)]
#[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
pub struct StringCells {
    present: PresenceBitmap,
    null: PresenceBitmap,
    spans: Vec<StringSpan>,
    bytes: Vec<u8>,
}

impl StringCells {
    const fn new() -> Self {
        Self {
            present: PresenceBitmap::new(),
            null: PresenceBitmap::new(),
            spans: Vec::new(),
            bytes: Vec::new(),
        }
    }

    fn get(&self, cell_index: usize) -> Cell<&str> {
        if !self.present.contains(cell_index) {
            return Cell::Absent;
        }

        if self.null.contains(cell_index) {
            return Cell::Null;
        }

        Cell::Value(self.str_at(self.dense_rank(cell_index)))
    }

    fn set(&mut self, cell_index: usize, value: &str) {
        let was_present = self.present.contains(cell_index);
        let was_null = was_present && self.null.contains(cell_index);
        let rank = self.dense_rank(cell_index);

        self.present.set(cell_index);
        self.null.clear(cell_index);

        if was_present && !was_null {
            self.remove_dense(rank);
        }

        self.insert_dense(rank, value);
    }

    fn set_null(&mut self, cell_index: usize) {
        let was_present = self.present.contains(cell_index);
        let was_null = was_present && self.null.contains(cell_index);

        if was_present && !was_null {
            let rank = self.dense_rank(cell_index);

            self.remove_dense(rank);
        }

        self.present.set(cell_index);
        self.null.set(cell_index);
    }

    fn remove(&mut self, cell_index: usize) -> bool {
        if !self.present.contains(cell_index) {
            return false;
        }

        if !self.null.contains(cell_index) {
            let rank = self.dense_rank(cell_index);

            self.remove_dense(rank);
        }

        self.present.clear(cell_index);
        self.null.clear(cell_index);

        true
    }

    fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    fn iter(&self) -> impl Iterator<Item = (usize, Option<&str>)> + '_ {
        let mut dense_position = 0usize;

        self.present.iter_present().map(move |cell_index| {
            if self.null.contains(cell_index) {
                (cell_index, None)
            } else {
                let value = self.str_at(dense_position);
                dense_position += 1;

                (cell_index, Some(value))
            }
        })
    }

    fn dense_rank(&self, cell_index: usize) -> usize {
        self.present.rank(cell_index) - self.null.rank(cell_index)
    }

    fn str_at(&self, dense_rank: usize) -> &str {
        let span = &self.spans[dense_rank];

        std::str::from_utf8(&self.bytes[span.start as usize..span.end as usize])
            .expect("String bytes must be valid UTF-8.")
    }

    fn insert_dense(&mut self, rank: usize, value: &str) {
        let start = u32::try_from(self.bytes.len()).expect("String bytes must fit in u32.");

        self.bytes.extend_from_slice(value.as_bytes());

        let end = u32::try_from(self.bytes.len()).expect("String bytes must fit in u32.");

        self.spans.insert(rank, StringSpan { start, end });
    }

    fn remove_dense(&mut self, rank: usize) {
        let removed_span = self.spans.remove(rank);
        let removed_length = removed_span.end - removed_span.start;

        self.bytes.splice(
            removed_span.start as usize..removed_span.end as usize,
            std::iter::empty(),
        );

        for span in &mut self.spans {
            if span.start > removed_span.start {
                span.start -= removed_length;
                span.end -= removed_length;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::AttributeChunk;
    use crate::graphrecord::datatypes::{Value, ValueView};
    use chrono::{NaiveDateTime, TimeDelta};

    fn create_chunk_with_three_strings() -> AttributeChunk {
        let mut chunk = AttributeChunk::new(&Value::String("lorem".to_string()));

        chunk.set(0, &Value::String("lorem".to_string()));
        chunk.set(1, &Value::String("do".to_string()));
        chunk.set(2, &Value::String("consectetur".to_string()));

        chunk
    }

    #[test]
    fn test_new() {
        let chunk = AttributeChunk::new(&Value::Int(0));

        assert!(matches!(chunk, AttributeChunk::Int(_)));
        assert!(chunk.is_empty());

        let chunk = AttributeChunk::new(&Value::Float(0.0));

        assert!(matches!(chunk, AttributeChunk::Float(_)));
        assert!(chunk.is_empty());

        let chunk = AttributeChunk::new(&Value::Bool(false));

        assert!(matches!(chunk, AttributeChunk::Bool(_)));
        assert!(chunk.is_empty());

        let chunk = AttributeChunk::new(&Value::DateTime(NaiveDateTime::MIN));

        assert!(matches!(chunk, AttributeChunk::DateTime(_)));
        assert!(chunk.is_empty());

        let chunk = AttributeChunk::new(&Value::Duration(TimeDelta::seconds(0)));

        assert!(matches!(chunk, AttributeChunk::Duration(_)));
        assert!(chunk.is_empty());

        let chunk = AttributeChunk::new(&Value::String(String::new()));

        assert!(matches!(chunk, AttributeChunk::String(_)));
        assert!(chunk.is_empty());

        let chunk = AttributeChunk::new(&Value::Null);

        assert!(matches!(chunk, AttributeChunk::Mixed(_)));
        assert!(chunk.is_empty());
    }

    #[test]
    fn test_get() {
        let mut chunk = AttributeChunk::new(&Value::Int(0));

        assert_eq!(None, chunk.get(2));

        chunk.set(0, &Value::Int(1));
        chunk.set(1, &Value::Null);

        assert_eq!(Some(ValueView::Int(1)), chunk.get(0));
        assert_eq!(Some(ValueView::Null), chunk.get(1));
        assert_eq!(None, chunk.get(2));

        let mut chunk = AttributeChunk::new(&Value::Int(0));

        chunk.set(3, &Value::Int(42));

        assert_eq!(Some(ValueView::Int(42)), chunk.get(3));

        let mut chunk = AttributeChunk::new(&Value::Float(0.0));

        chunk.set(1, &Value::Float(1.5));

        assert_eq!(Some(ValueView::Float(1.5)), chunk.get(1));

        let mut chunk = AttributeChunk::new(&Value::Bool(false));

        chunk.set(0, &Value::Bool(true));
        chunk.set(1, &Value::Bool(false));

        assert_eq!(Some(ValueView::Bool(true)), chunk.get(0));
        assert_eq!(Some(ValueView::Bool(false)), chunk.get(1));

        let mut chunk = AttributeChunk::new(&Value::DateTime(NaiveDateTime::MIN));

        chunk.set(0, &Value::DateTime(NaiveDateTime::MAX));

        assert_eq!(Some(ValueView::DateTime(NaiveDateTime::MAX)), chunk.get(0));

        let mut chunk = AttributeChunk::new(&Value::Duration(TimeDelta::seconds(0)));

        chunk.set(0, &Value::Duration(TimeDelta::seconds(5)));

        assert_eq!(
            Some(ValueView::Duration(TimeDelta::seconds(5))),
            chunk.get(0)
        );

        let mut chunk = AttributeChunk::new(&Value::String(String::new()));

        chunk.set(0, &Value::String(String::new()));
        chunk.set(1, &Value::String("lorem".to_string()));

        assert_eq!(Some(ValueView::String("".into())), chunk.get(0));
        assert_eq!(Some(ValueView::String("lorem".into())), chunk.get(1));

        let mut chunk = AttributeChunk::new(&Value::Null);

        chunk.set(0, &Value::Int(1));
        chunk.set(1, &Value::String("lorem".to_string()));
        chunk.set(2, &Value::Bool(true));

        assert_eq!(Some(ValueView::Int(1)), chunk.get(0));
        assert_eq!(Some(ValueView::String("lorem".into())), chunk.get(1));
        assert_eq!(Some(ValueView::Bool(true)), chunk.get(2));
    }

    #[test]
    fn test_set() {
        let mut chunk = create_chunk_with_three_strings();

        chunk.set(0, &Value::String("adipiscing".to_string()));

        assert_eq!(Some(ValueView::String("adipiscing".into())), chunk.get(0));
        assert_eq!(Some(ValueView::String("do".into())), chunk.get(1));
        assert_eq!(Some(ValueView::String("consectetur".into())), chunk.get(2));

        let mut chunk = AttributeChunk::new(&Value::Int(0));

        chunk.set(3, &Value::Int(10));
        chunk.set(5, &Value::Null);
        chunk.set(7, &Value::Int(20));

        let AttributeChunk::Int(scalar_cells) = &chunk else {
            panic!("Chunk must be Int.");
        };

        assert_eq!(2, scalar_cells.cells.len());
        assert_eq!(Some(ValueView::Int(10)), chunk.get(3));
        assert_eq!(Some(ValueView::Null), chunk.get(5));
        assert_eq!(Some(ValueView::Int(20)), chunk.get(7));

        let mut chunk = AttributeChunk::new(&Value::Int(0));

        chunk.set(0, &Value::Int(10));
        chunk.set(1, &Value::Int(20));

        chunk.set(2, &Value::String("lorem".to_string()));

        assert!(matches!(chunk, AttributeChunk::Mixed(_)));
        assert_eq!(Some(ValueView::Int(10)), chunk.get(0));
        assert_eq!(Some(ValueView::Int(20)), chunk.get(1));
        assert_eq!(Some(ValueView::String("lorem".into())), chunk.get(2));

        let mut chunk = AttributeChunk::new(&Value::Bool(true));

        chunk.set(0, &Value::Bool(true));
        chunk.set(1, &Value::Null);
        chunk.set(2, &Value::Bool(false));

        chunk.set(3, &Value::Int(7));

        assert!(matches!(chunk, AttributeChunk::Mixed(_)));
        assert_eq!(Some(ValueView::Bool(true)), chunk.get(0));
        assert_eq!(Some(ValueView::Null), chunk.get(1));
        assert_eq!(Some(ValueView::Bool(false)), chunk.get(2));
        assert_eq!(Some(ValueView::Int(7)), chunk.get(3));

        let mut chunk = AttributeChunk::new(&Value::String(String::new()));

        chunk.set(0, &Value::String("lorem".to_string()));
        chunk.set(1, &Value::Null);
        chunk.set(2, &Value::String("ipsum".to_string()));

        chunk.set(3, &Value::Bool(true));

        assert!(matches!(chunk, AttributeChunk::Mixed(_)));
        assert_eq!(Some(ValueView::String("lorem".into())), chunk.get(0));
        assert_eq!(Some(ValueView::Null), chunk.get(1));
        assert_eq!(Some(ValueView::String("ipsum".into())), chunk.get(2));
        assert_eq!(Some(ValueView::Bool(true)), chunk.get(3));

        let mut chunk = AttributeChunk::new(&Value::Null);

        chunk.set(0, &Value::Int(1));
        chunk.set(1, &Value::String("lorem".to_string()));
        chunk.set(2, &Value::Bool(true));

        assert_eq!(Some(ValueView::Int(1)), chunk.get(0));
        assert_eq!(Some(ValueView::String("lorem".into())), chunk.get(1));
        assert_eq!(Some(ValueView::Bool(true)), chunk.get(2));

        let mut chunk = AttributeChunk::new(&Value::Null);

        chunk.set(0, &Value::Int(1));
        chunk.set(1, &Value::Null);

        assert_eq!(Some(ValueView::Null), chunk.get(1));

        let AttributeChunk::Mixed(scalar_cells) = &chunk else {
            panic!("Chunk must be Mixed.");
        };

        assert_eq!(1, scalar_cells.cells.len());
        assert!(
            scalar_cells
                .cells
                .iter()
                .all(|value| !matches!(value, Value::Null))
        );
    }

    #[test]
    fn test_remove() {
        let mut chunk = AttributeChunk::new(&Value::Int(0));

        chunk.set(3, &Value::Int(42));

        assert!(chunk.remove(3));

        assert_eq!(None, chunk.get(3));
        assert!(!chunk.remove(3));

        let mut chunk = AttributeChunk::new(&Value::Float(0.0));

        chunk.set(1, &Value::Float(1.5));

        assert!(chunk.remove(1));

        assert_eq!(None, chunk.get(1));

        let mut chunk = AttributeChunk::new(&Value::Bool(false));

        chunk.set(0, &Value::Bool(true));
        chunk.set(1, &Value::Bool(false));

        assert!(chunk.remove(0));

        assert_eq!(None, chunk.get(0));
        assert_eq!(Some(ValueView::Bool(false)), chunk.get(1));

        let mut chunk = AttributeChunk::new(&Value::DateTime(NaiveDateTime::MIN));

        chunk.set(0, &Value::DateTime(NaiveDateTime::MAX));

        assert!(chunk.remove(0));

        assert_eq!(None, chunk.get(0));

        let mut chunk = AttributeChunk::new(&Value::Duration(TimeDelta::seconds(0)));

        chunk.set(0, &Value::Duration(TimeDelta::seconds(5)));

        assert!(chunk.remove(0));

        assert_eq!(None, chunk.get(0));

        let mut chunk = AttributeChunk::new(&Value::String("lorem".to_string()));

        chunk.set(0, &Value::String("lorem".to_string()));

        assert!(chunk.remove(0));

        assert_eq!(None, chunk.get(0));

        let mut chunk = AttributeChunk::new(&Value::Int(0));

        chunk.set(3, &Value::Int(10));
        chunk.set(5, &Value::Null);
        chunk.set(7, &Value::Int(20));

        assert!(chunk.remove(3));

        assert_eq!(Some(ValueView::Int(20)), chunk.get(7));
        assert_eq!(Some(ValueView::Null), chunk.get(5));
        assert_eq!(None, chunk.get(3));

        let mut chunk = create_chunk_with_three_strings();

        assert!(chunk.remove(1));

        assert_eq!(Some(ValueView::String("lorem".into())), chunk.get(0));
        assert_eq!(None, chunk.get(1));
        assert_eq!(Some(ValueView::String("consectetur".into())), chunk.get(2));

        let mut chunk = AttributeChunk::new(&Value::String(String::new()));

        chunk.set(0, &Value::String(String::new()));
        chunk.set(1, &Value::String("lorem".to_string()));

        assert!(chunk.remove(0));

        assert_eq!(None, chunk.get(0));
        assert_eq!(Some(ValueView::String("lorem".into())), chunk.get(1));
    }

    #[test]
    fn test_is_empty() {
        let mut chunk = AttributeChunk::new(&Value::Int(0));

        assert!(chunk.is_empty());

        chunk.set(3, &Value::Int(42));

        assert!(!chunk.is_empty());

        assert!(chunk.remove(3));

        assert!(chunk.is_empty());
    }
}
