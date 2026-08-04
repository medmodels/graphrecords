use crate::Diagnostic;
use graphrecords_core::graphrecord::GraphRecordValue;
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

#[derive(Debug)]
pub struct IntegerOverflow<T> {
    value: T,
}

impl<T> IntegerOverflow<T> {
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }
}

impl<T: Display> Display for IntegerOverflow<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "`{}` exceeds the representable integer range",
            self.value
        )
    }
}

impl<T: Debug + Display> Error for IntegerOverflow<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for IntegerOverflow<T> {
    fn name() -> &'static str {
        "IntegerOverflow"
    }
}

#[derive(Debug)]
pub struct InvalidClipBounds<T> {
    lower: T,
    upper: T,
}

impl<T> InvalidClipBounds<T> {
    #[must_use]
    pub const fn new(lower: T, upper: T) -> Self {
        Self { lower, upper }
    }

    #[must_use]
    pub const fn lower(&self) -> &T {
        &self.lower
    }

    #[must_use]
    pub const fn upper(&self) -> &T {
        &self.upper
    }
}

impl<T: Display> Display for InvalidClipBounds<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "lower clip bound `{}` exceeds upper clip bound `{}`",
            self.lower, self.upper
        )
    }
}

impl<T: Debug + Display> Error for InvalidClipBounds<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for InvalidClipBounds<T> {
    fn name() -> &'static str {
        "InvalidClipBounds"
    }

    fn help(&self) -> Option<String> {
        Some("provide a lower bound that does not exceed the upper bound".to_string())
    }
}

#[derive(Debug)]
pub struct NegativeLength {
    value: i64,
}

impl NegativeLength {
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> i64 {
        self.value
    }
}

impl Display for NegativeLength {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "`{}` is a negative length", self.value)
    }
}

impl Error for NegativeLength {}

impl Diagnostic for NegativeLength {
    fn name() -> &'static str {
        "NegativeLength"
    }
}

#[derive(Debug)]
pub struct NegativeSquareRoot {
    value: GraphRecordValue,
}

impl NegativeSquareRoot {
    #[must_use]
    pub const fn new(value: GraphRecordValue) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &GraphRecordValue {
        &self.value
    }
}

impl Display for NegativeSquareRoot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot take the square root of negative `{}`",
            self.value
        )
    }
}

impl Error for NegativeSquareRoot {}

impl Diagnostic for NegativeSquareRoot {
    fn name() -> &'static str {
        "NegativeSquareRoot"
    }
}

#[derive(Debug)]
pub struct NonIntegerValue<T> {
    value: T,
}

impl<T> NonIntegerValue<T> {
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }
}

impl<T: Display> Display for NonIntegerValue<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "`{}` is not an integer value", self.value)
    }
}

impl<T: Debug + Display> Error for NonIntegerValue<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for NonIntegerValue<T> {
    fn name() -> &'static str {
        "NonIntegerValue"
    }
}

#[derive(Debug)]
pub struct NonNumericValue<T> {
    value: T,
}

impl<T> NonNumericValue<T> {
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }
}

impl<T: Display> Display for NonNumericValue<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "`{}` is not a numeric value", self.value)
    }
}

impl<T: Debug + Display> Error for NonNumericValue<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for NonNumericValue<T> {
    fn name() -> &'static str {
        "NonNumericValue"
    }
}

#[derive(Debug)]
pub struct NonPositiveLogarithm {
    value: GraphRecordValue,
}

impl NonPositiveLogarithm {
    #[must_use]
    pub const fn new(value: GraphRecordValue) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &GraphRecordValue {
        &self.value
    }
}

impl Display for NonPositiveLogarithm {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot take the logarithm of non-positive `{}`",
            self.value
        )
    }
}

impl Error for NonPositiveLogarithm {}

impl Diagnostic for NonPositiveLogarithm {
    fn name() -> &'static str {
        "NonPositiveLogarithm"
    }
}
