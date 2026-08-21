use crate::Diagnostic;
use graphrecords_core::graphrecord::Value;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct InvalidMedianValue {
    value: Value,
}

impl InvalidMedianValue {
    #[must_use]
    pub const fn new(value: Value) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }
}

impl Display for InvalidMedianValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot calculate the median of value `{}`",
            self.value
        )
    }
}

impl Error for InvalidMedianValue {}

impl Diagnostic for InvalidMedianValue {
    fn name() -> &'static str {
        "InvalidMedianValue"
    }

    fn help(&self) -> Option<String> {
        Some(
            "narrow the values down first using is_int(), is_float(), is_datetime() or is_duration()"
                .to_string(),
        )
    }
}

#[derive(Debug)]
pub struct InvalidStandardDeviationValue {
    value: Value,
}

impl InvalidStandardDeviationValue {
    #[must_use]
    pub const fn new(value: Value) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &Value {
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

#[derive(Debug)]
pub struct InvalidVarianceValue {
    value: Value,
}

impl InvalidVarianceValue {
    #[must_use]
    pub const fn new(value: Value) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }
}

impl Display for InvalidVarianceValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot calculate variance of value `{}`",
            self.value
        )
    }
}

impl Error for InvalidVarianceValue {}

impl Diagnostic for InvalidVarianceValue {
    fn name() -> &'static str {
        "InvalidVarianceValue"
    }

    fn help(&self) -> Option<String> {
        Some("narrow the values down first using is_int() or is_float()".to_string())
    }
}
