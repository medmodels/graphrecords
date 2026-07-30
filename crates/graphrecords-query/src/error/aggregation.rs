use crate::Diagnostic;
use graphrecords_core::graphrecord::GraphRecordValue;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct InvalidStandardDeviationValue {
    value: GraphRecordValue,
}

impl InvalidStandardDeviationValue {
    #[must_use]
    pub const fn new(value: GraphRecordValue) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &GraphRecordValue {
        &self.value
    }
}

impl Display for InvalidStandardDeviationValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot calculate standard deviation of value `{}`",
            self.value
        )
    }
}

impl Error for InvalidStandardDeviationValue {}

impl Diagnostic for InvalidStandardDeviationValue {
    fn name() -> &'static str {
        "InvalidStandardDeviationValue"
    }

    fn help(&self) -> Option<String> {
        Some("narrow the values down first using is_int() or is_float()".to_string())
    }
}
