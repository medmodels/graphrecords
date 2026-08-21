use graphrecords_core::prelude::{DataType, Value};
use itertools::Itertools;

#[derive(Debug, Clone)]
pub enum AttributeOverviewData {
    Categorical { distinct_values: Vec<Value> },
    Continuous { min: Value, mean: Value, max: Value },
    Temporal { min: Value, max: Value },
    Unstructured { distinct_count: usize },
}

impl AttributeOverviewData {
    pub(crate) fn categorical(distinct_values: Vec<Value>) -> Self {
        Self::Categorical {
            distinct_values: distinct_values
                .into_iter()
                .sorted_by(Value::total_cmp)
                .collect(),
        }
    }

    pub(crate) fn continuous(
        minimum: Option<Value>,
        mean: Option<Value>,
        maximum: Option<Value>,
    ) -> Self {
        Self::Continuous {
            min: minimum.unwrap_or(Value::Null),
            mean: mean.unwrap_or(Value::Null),
            max: maximum.unwrap_or(Value::Null),
        }
    }

    pub(crate) fn temporal(minimum: Option<Value>, maximum: Option<Value>) -> Self {
        Self::Temporal {
            min: minimum.unwrap_or(Value::Null),
            max: maximum.unwrap_or(Value::Null),
        }
    }

    pub(crate) fn unstructured(distinct_count: Option<Value>) -> Self {
        let distinct_count = distinct_count
            .map_or(Some(0), |value| match value {
                Value::Int(count) => usize::try_from(count).ok(),
                _ => None,
            })
            .expect("Unique count must be a non-negative Int.");

        Self::Unstructured { distinct_count }
    }

    pub(crate) const fn attribute_type_name(&self) -> &'static str {
        match self {
            Self::Categorical { .. } => "Categorical",
            Self::Continuous { .. } => "Continuous",
            Self::Temporal { .. } => "Temporal",
            Self::Unstructured { .. } => "Unstructured",
        }
    }

    pub(crate) fn details(&self) -> String {
        match self {
            Self::Categorical { distinct_values } => {
                format!(
                    "Distinct values: [{}]",
                    distinct_values
                        .iter()
                        .map(std::string::ToString::to_string)
                        .join(", ")
                )
            }
            Self::Continuous { min, mean, max } => {
                format!("Min: {min}\nMean: {mean}\nMax: {max}")
            }
            Self::Temporal { min, max } => {
                format!("Min: {min}\nMax: {max}")
            }
            Self::Unstructured { distinct_count } => {
                format!("Distinct value count: {distinct_count}")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AttributeOverview {
    pub data_type: DataType,
    pub data: AttributeOverviewData,
}
