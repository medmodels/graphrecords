use super::stats::Stats;

#[derive(Clone, Debug)]
pub struct Estimate {
    pub elements: Option<usize>,
    pub distinct: Option<usize>,
    pub selectivity: Option<f64>,
    pub per_group: Option<Box<Self>>,
}

impl Estimate {
    pub const UNKNOWN: Self = Self {
        elements: None,
        distinct: None,
        selectivity: None,
        per_group: None,
    };

    #[must_use]
    pub fn values(elements: usize, distinct: usize) -> Self {
        Self {
            elements: Some(elements),
            distinct: Some(distinct.min(elements)),
            selectivity: None,
            per_group: None,
        }
    }

    #[must_use]
    pub fn singleton() -> Self {
        Self::values(1, 1)
    }

    #[must_use]
    pub fn scaled(self, selectivity: f64) -> Self {
        let elements = self
            .elements
            .map(|elements| (elements as f64 * selectivity).round() as usize);
        let distinct = match (self.distinct, elements) {
            (Some(distinct), Some(elements)) => Some(distinct.min(elements)),
            (distinct, _) => distinct,
        };

        Self {
            elements,
            distinct,
            ..self
        }
    }
}

pub trait Estimated {
    fn estimate(&self, stats: &Stats) -> Estimate;
}
