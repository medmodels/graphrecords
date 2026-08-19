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
