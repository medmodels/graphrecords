use crate::{AttributeName, Diagnostic, Failure, IndexValue, QueryResult, Scalar, ValueType};
use graphrecords_core::graphrecord::{GraphRecordAttribute, GraphRecordValue, NodeIndex};
use regex::Error as RegexError;
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

pub trait StringValue: ValueType {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a;

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a;
}

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
    pub value: String,
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
    pub pattern: String,
    pub error: RegexError,
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
    pub start: usize,
    pub end: usize,
    pub length: usize,
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
    pub value: T,
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
    pub length: usize,
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
    pub width: usize,
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

impl StringValue for Scalar {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordValue::String(value) => Ok(value),
            value => Err(Failure::new(label, NonStringValue { value })),
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordValue::String(value)
    }
}

impl StringValue for AttributeName {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordAttribute::String(value) => Ok(value),
            value @ GraphRecordAttribute::Int(_) => {
                Err(Failure::new(label, NonStringValue { value }))
            }
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordAttribute::String(value)
    }
}

impl StringValue for IndexValue<NodeIndex> {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordAttribute::String(value) => Ok(value),
            value @ GraphRecordAttribute::Int(_) => {
                Err(Failure::new(label, NonStringValue { value }))
            }
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordAttribute::String(value)
    }
}

impl StringValue for IndexValue<AttributeName> {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordAttribute::String(value) => Ok(value),
            value @ GraphRecordAttribute::Int(_) => {
                Err(Failure::new(label, NonStringValue { value }))
            }
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordAttribute::String(value)
    }
}

impl StringValue for IndexValue<GraphRecordValue> {
    fn into_string<'a>(label: &'static str, value: Self::Value<'a>) -> QueryResult<String>
    where
        Self: 'a,
    {
        match value {
            GraphRecordValue::String(value) => Ok(value),
            value => Err(Failure::new(label, NonStringValue { value })),
        }
    }

    fn from_string<'a>(value: String) -> Self::Value<'a>
    where
        Self: 'a,
    {
        GraphRecordValue::String(value)
    }
}
