pub mod aggregation;
pub mod argument;
pub mod arithmetic;
pub mod comparison;
pub mod conversion;
#[cfg(feature = "dynamic")]
pub mod dispatch;
pub mod execution;
pub mod grouping;
pub mod groups;
pub mod index;
pub mod numeric;
pub mod ordering;
pub mod policy;
pub mod string;
pub mod structure;

use crate::{IndexDomain, OwnedIndex};
use graphrecords_core::{GraphRecord, errors::GraphRecordError};
use std::{
    any::{Any, TypeId},
    error::Error,
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

pub type QueryResult<T> = Result<T, Box<Failure>>;

pub trait Diagnostic: Error + Send + Sync + 'static {
    fn name() -> &'static str
    where
        Self: Sized;

    fn help(&self) -> Option<String> {
        None
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FailureKind {
    identifier: TypeId,
    name: &'static str,
}

impl FailureKind {
    #[must_use]
    pub fn of<D: Diagnostic>() -> Self {
        Self {
            identifier: TypeId::of::<D>(),
            name: D::name(),
        }
    }

    #[must_use]
    pub fn is<D: Diagnostic>(&self) -> bool {
        self.identifier == TypeId::of::<D>()
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

impl Display for FailureKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name)
    }
}

impl PartialEq for FailureKind {
    fn eq(&self, other: &Self) -> bool {
        self.identifier == other.identifier
    }
}

impl Eq for FailureKind {}

impl Hash for FailureKind {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identifier.hash(state);
    }
}

pub trait ErrorGroup: 'static {
    fn name() -> &'static str
    where
        Self: Sized;

    fn contains(kind: &FailureKind) -> bool;
}

#[derive(Clone, Debug)]
pub struct Failure {
    operation: &'static str,
    element: Option<Arc<dyn OwnedIndex>>,
    kind: FailureKind,
    cause: Arc<dyn Diagnostic>,
}

impl Failure {
    #[must_use]
    pub fn new<D: Diagnostic>(cause: D, operation: &'static str) -> Box<Self> {
        Box::new(Self {
            operation,
            element: None,
            kind: FailureKind::of::<D>(),
            cause: Arc::new(cause),
        })
    }

    #[must_use]
    pub fn new_at<I: IndexDomain, D: Diagnostic>(
        cause: D,
        index: &I::Index<'_>,
        operation: &'static str,
    ) -> Box<Self> {
        Box::new(Self {
            operation,
            element: Some(Arc::new(I::own_index(index))),
            kind: FailureKind::of::<D>(),
            cause: Arc::new(cause),
        })
    }

    #[must_use]
    pub fn new_at_address<I: IndexDomain, D: Diagnostic>(
        cause: D,
        graphrecord: &GraphRecord,
        address: &I::Address,
        operation: &'static str,
    ) -> Box<Self> {
        Self::new_at::<I, _>(cause, &I::index(graphrecord, address), operation)
    }

    #[must_use]
    pub fn at<I: IndexDomain>(mut self: Box<Self>, index: &I::Index<'_>) -> Box<Self> {
        self.element = Some(Arc::new(I::own_index(index)));
        self
    }

    #[must_use]
    pub fn at_address<I: IndexDomain>(
        self: Box<Self>,
        graphrecord: &GraphRecord,
        address: &I::Address,
    ) -> Box<Self> {
        self.at::<I>(&I::index(graphrecord, address))
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

        element.downcast_ref()
    }

    #[must_use]
    pub fn cause(&self) -> &dyn Diagnostic {
        self.cause.as_ref()
    }

    #[must_use]
    pub const fn kind(&self) -> FailureKind {
        self.kind
    }

    #[must_use]
    pub fn is_kind<D: Diagnostic>(&self) -> bool {
        self.kind.is::<D>()
    }

    #[must_use]
    pub fn help(&self) -> Option<String> {
        self.cause.help()
    }

    #[must_use]
    pub fn downcast_cause<T: Error + 'static>(&self) -> Option<&T> {
        let mut current: &(dyn Error + 'static) = self.cause.as_ref();

        loop {
            if let Some(cause) = current.downcast_ref() {
                return Some(cause);
            }

            if let Some(bundle) = current.downcast_ref::<policy::RaisedFailures>() {
                return bundle.failures().iter().find_map(Self::downcast_cause::<T>);
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
pub struct External<E: Error + Send + Sync + 'static>(E);

impl<E: Error + Send + Sync + 'static> External<E> {
    #[must_use]
    pub const fn new(error: E) -> Self {
        Self(error)
    }

    #[must_use]
    pub const fn error(&self) -> &E {
        &self.0
    }
}

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

impl<E: Error + Send + Sync + 'static> Diagnostic for External<E> {
    fn name() -> &'static str {
        "External"
    }
}

impl From<Box<Failure>> for GraphRecordError {
    fn from(failure: Box<Failure>) -> Self {
        Self::QueryFailure {
            cause: Arc::new(*failure),
        }
    }
}

impl Diagnostic for GraphRecordError {
    fn name() -> &'static str {
        "GraphRecordError"
    }

    fn help(&self) -> Option<String> {
        match self {
            Self::IncompatibleValueOperands { .. } | Self::IncompatibleIdentifierOperands { .. } => {
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
