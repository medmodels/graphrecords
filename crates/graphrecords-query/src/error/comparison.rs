use crate::{Diagnostic, OwnedIndex};
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

#[derive(Debug)]
pub struct IncomparableValues<T> {
    first: T,
    second: T,
}

impl<T> IncomparableValues<T> {
    #[must_use]
    pub const fn new(first: T, second: T) -> Self {
        Self { first, second }
    }

    #[must_use]
    pub const fn first(&self) -> &T {
        &self.first
    }

    #[must_use]
    pub const fn second(&self) -> &T {
        &self.second
    }
}

impl<T: Display> Display for IncomparableValues<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot compare values `{}` and `{}`",
            self.first, self.second
        )
    }
}

impl<T: Debug + Display> Error for IncomparableValues<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for IncomparableValues<T> {
    fn name() -> &'static str {
        "IncomparableValues"
    }

    fn help(&self) -> Option<String> {
        Some(
            "narrow the values down first using is_string(), is_int(), is_float(), is_bool(), is_datetime() or is_duration()"
                .to_string(),
        )
    }
}

#[derive(Debug)]
pub struct IncomparableValuesAt<V, E: OwnedIndex> {
    first: V,
    second: V,
    first_element: E,
    second_element: E,
}

impl<V, E: OwnedIndex> IncomparableValuesAt<V, E> {
    #[must_use]
    pub const fn new(first: V, second: V, first_element: E, second_element: E) -> Self {
        Self {
            first,
            second,
            first_element,
            second_element,
        }
    }

    #[must_use]
    pub const fn first(&self) -> &V {
        &self.first
    }

    #[must_use]
    pub const fn second(&self) -> &V {
        &self.second
    }

    #[must_use]
    pub const fn first_element(&self) -> &E {
        &self.first_element
    }

    #[must_use]
    pub const fn second_element(&self) -> &E {
        &self.second_element
    }
}

impl<V: Display, E: OwnedIndex> Display for IncomparableValuesAt<V, E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot compare value `{}` at element `{}` with value `{}` at element `{}`",
            self.first, self.first_element, self.second, self.second_element
        )
    }
}

impl<V: Debug + Display, E: OwnedIndex> Error for IncomparableValuesAt<V, E> {}

impl<V: Debug + Display + Send + Sync + 'static, E: OwnedIndex> Diagnostic
    for IncomparableValuesAt<V, E>
{
    fn name() -> &'static str {
        "IncomparableValuesAt"
    }

    fn help(&self) -> Option<String> {
        Some(
            "narrow the values down first using is_string(), is_int(), is_float(), is_bool(), is_datetime() or is_duration()"
                .to_string(),
        )
    }
}
