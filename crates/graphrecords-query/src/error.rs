use crate::{OwnedIndex, ToOwnedValue};
use graphrecords_core::errors::GraphRecordError;
use std::{
    any::Any,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

pub type QueryResult<T> = Result<T, Box<Failure>>;

pub trait Diagnostic: Error + Send + Sync + 'static {
    fn help(&self) -> Option<String> {
        None
    }
}

#[derive(Clone, Debug)]
pub struct Failure {
    operation: &'static str,
    element: Option<Arc<dyn OwnedIndex>>,
    cause: Arc<dyn Diagnostic>,
}

impl Failure {
    pub fn new(operation: &'static str, cause: impl Diagnostic) -> Box<Self> {
        Box::new(Self {
            operation,
            element: None,
            cause: Arc::new(cause),
        })
    }

    pub fn new_at(
        operation: &'static str,
        cause: impl Diagnostic,
        element: &impl ToOwnedValue<Owned: OwnedIndex>,
    ) -> Box<Self> {
        Box::new(Self {
            operation,
            element: Some(Arc::new(element.to_owned_value())),
            cause: Arc::new(cause),
        })
    }

    #[must_use]
    pub const fn operation(&self) -> &'static str {
        self.operation
    }

    #[must_use]
    pub fn element(&self) -> Option<&dyn OwnedIndex> {
        self.element.as_deref()
    }

    #[must_use]
    pub fn downcast_element<T: OwnedIndex>(&self) -> Option<&T> {
        let element: &dyn Any = self.element.as_deref()?;

        element.downcast_ref::<T>()
    }

    #[must_use]
    pub fn cause(&self) -> &dyn Diagnostic {
        self.cause.as_ref()
    }

    #[must_use]
    pub fn help(&self) -> Option<String> {
        self.cause.help()
    }

    #[must_use]
    pub fn downcast_cause<T: Error + 'static>(&self) -> Option<&T> {
        let mut current: &(dyn Error + 'static) = self.cause.as_ref();

        loop {
            if let Some(cause) = current.downcast_ref::<T>() {
                return Some(cause);
            }

            current = current.source()?;
        }
    }

    #[must_use]
    pub fn has_cause<T: Error + 'static>(&self) -> bool {
        self.downcast_cause::<T>().is_some()
    }
}

impl Display for Failure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.element() {
            Some(element) => write!(
                formatter,
                "operation `{}` failed at element `{element}`: {}",
                self.operation, self.cause,
            )?,
            None => write!(
                formatter,
                "operation `{}` failed: {}",
                self.operation, self.cause,
            )?,
        }

        if let Some(help) = self.help() {
            write!(formatter, "\nhelp: {help}")?;
        }

        Ok(())
    }
}

impl Error for Failure {}

#[derive(Debug)]
pub struct External<E: Error + Send + Sync + 'static>(pub E);

impl<E: Error + Send + Sync + 'static> Display for External<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl<E: Error + Send + Sync + 'static> Error for External<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

impl<E: Error + Send + Sync + 'static> Diagnostic for External<E> {}

#[derive(Debug)]
pub struct IncomparableValues<T> {
    pub first: T,
    pub second: T,
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
    fn help(&self) -> Option<String> {
        Some(
            "narrow the values down first using is_string(), is_int(), is_float(), is_bool(), is_datetime() or is_duration()"
                .to_string(),
        )
    }
}

#[derive(Debug)]
pub struct IncomparableValuesAt<V, E: OwnedIndex> {
    pub first: V,
    pub second: V,
    pub first_element: E,
    pub second_element: E,
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
    fn help(&self) -> Option<String> {
        Some(
            "narrow the values down first using is_string(), is_int(), is_float(), is_bool(), is_datetime() or is_duration()"
                .to_string(),
        )
    }
}

impl Diagnostic for GraphRecordError {
    fn help(&self) -> Option<String> {
        match self {
            Self::IncompatibleValueOperands { .. } | Self::IncompatibleAttributeOperands { .. } => {
                Some(
                    "narrow the values down first using is_string(), is_int(), is_float(), is_bool(), is_datetime() or is_duration()"
                        .to_string(),
                )
            }
            Self::GroupNotFound { .. } => {
                Some("add the group first or check `groups()` before querying".to_string())
            }
            _ => None,
        }
    }
}
