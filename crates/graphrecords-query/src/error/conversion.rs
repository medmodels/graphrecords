use crate::Diagnostic;
use graphrecords_core::graphrecord::datatypes::DataType;
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

#[derive(Debug)]
pub struct InvalidCast<T> {
    value: T,
    target: DataType,
}

impl<T> InvalidCast<T> {
    #[must_use]
    pub const fn new(value: T, target: DataType) -> Self {
        Self { value, target }
    }

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    #[must_use]
    pub const fn target(&self) -> &DataType {
        &self.target
    }
}

impl<T: Display> Display for InvalidCast<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "cannot cast `{}` to {}", self.value, self.target)
    }
}

impl<T: Debug + Display> Error for InvalidCast<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for InvalidCast<T> {
    fn name() -> &'static str {
        "InvalidCast"
    }
}

#[derive(Debug)]
pub struct InvalidTransition<T> {
    value: T,
    target: &'static str,
}

impl<T> InvalidTransition<T> {
    #[must_use]
    pub const fn new(value: T, target: &'static str) -> Self {
        Self { value, target }
    }

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    #[must_use]
    pub const fn target(&self) -> &'static str {
        self.target
    }
}

impl<T: Display> Display for InvalidTransition<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot represent `{}` as {}",
            self.value, self.target
        )
    }
}

impl<T: Debug + Display> Error for InvalidTransition<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for InvalidTransition<T> {
    fn name() -> &'static str {
        "InvalidTransition"
    }
}
