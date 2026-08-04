use crate::Diagnostic;
use regex::Error as RegexError;
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

#[derive(Debug)]
pub struct EmptySplitDelimiter;

impl Display for EmptySplitDelimiter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "split delimiter cannot be empty")
    }
}

impl Error for EmptySplitDelimiter {}

impl Diagnostic for EmptySplitDelimiter {
    fn name() -> &'static str {
        "EmptySplitDelimiter"
    }
}

#[derive(Debug)]
pub struct InvalidPaddingCharacter {
    value: String,
}

impl InvalidPaddingCharacter {
    #[must_use]
    pub const fn new(value: String) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &str {
        self.value.as_str()
    }
}

impl Display for InvalidPaddingCharacter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "padding requires exactly one character, got `{}`",
            self.value
        )
    }
}

impl Error for InvalidPaddingCharacter {}

impl Diagnostic for InvalidPaddingCharacter {
    fn name() -> &'static str {
        "InvalidPaddingCharacter"
    }
}

#[derive(Debug)]
pub struct InvalidRegexPattern {
    pattern: String,
    error: RegexError,
}

impl InvalidRegexPattern {
    #[must_use]
    pub const fn new(pattern: String, error: RegexError) -> Self {
        Self { pattern, error }
    }

    #[must_use]
    pub const fn pattern(&self) -> &str {
        self.pattern.as_str()
    }

    #[must_use]
    pub const fn error(&self) -> &RegexError {
        &self.error
    }
}

impl Display for InvalidRegexPattern {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "`{}` is not a valid regular expression: {}",
            self.pattern, self.error
        )
    }
}

impl Error for InvalidRegexPattern {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

impl Diagnostic for InvalidRegexPattern {
    fn name() -> &'static str {
        "InvalidRegexPattern"
    }
}

#[derive(Debug)]
pub struct InvalidStringSlice {
    start: usize,
    end: usize,
    length: usize,
}

impl InvalidStringSlice {
    #[must_use]
    pub const fn new(start: usize, end: usize, length: usize) -> Self {
        Self { start, end, length }
    }

    #[must_use]
    pub const fn start(&self) -> usize {
        self.start
    }

    #[must_use]
    pub const fn end(&self) -> usize {
        self.end
    }

    #[must_use]
    pub const fn length(&self) -> usize {
        self.length
    }
}

impl Display for InvalidStringSlice {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "character range {}..{} is invalid for a string of length {}",
            self.start, self.end, self.length
        )
    }
}

impl Error for InvalidStringSlice {}

impl Diagnostic for InvalidStringSlice {
    fn name() -> &'static str {
        "InvalidStringSlice"
    }
}

#[derive(Debug)]
pub struct NonStringValue<T> {
    value: T,
}

impl<T> NonStringValue<T> {
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }
}

impl<T: Display> Display for NonStringValue<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "`{}` is not a string value", self.value)
    }
}

impl<T: Debug + Display> Error for NonStringValue<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for NonStringValue<T> {
    fn name() -> &'static str {
        "NonStringValue"
    }
}

#[derive(Debug)]
pub struct StringLengthOverflow {
    length: usize,
}

impl StringLengthOverflow {
    #[must_use]
    pub const fn new(length: usize) -> Self {
        Self { length }
    }

    #[must_use]
    pub const fn length(&self) -> usize {
        self.length
    }
}

impl Display for StringLengthOverflow {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "string character count `{}` does not fit in an integer value",
            self.length
        )
    }
}

impl Error for StringLengthOverflow {}

impl Diagnostic for StringLengthOverflow {
    fn name() -> &'static str {
        "StringLengthOverflow"
    }
}

#[derive(Debug)]
pub struct StringPaddingOverflow {
    width: usize,
}

impl StringPaddingOverflow {
    #[must_use]
    pub const fn new(width: usize) -> Self {
        Self { width }
    }

    #[must_use]
    pub const fn width(&self) -> usize {
        self.width
    }
}

impl Display for StringPaddingOverflow {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "padded string width `{}` exceeds the supported capacity",
            self.width
        )
    }
}

impl Error for StringPaddingOverflow {}

impl Diagnostic for StringPaddingOverflow {
    fn name() -> &'static str {
        "StringPaddingOverflow"
    }
}
