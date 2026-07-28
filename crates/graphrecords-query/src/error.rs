use crate::{IndexDomain, OwnedIndex};
use graphrecords_core::errors::GraphRecordError;
use std::{
    any::{Any, TypeId},
    cmp::Ordering,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
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

impl PartialOrd for FailureKind {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FailureKind {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.identifier == other.identifier {
            return Ordering::Equal;
        }

        self.name
            .cmp(other.name)
            .then_with(|| self.identifier.cmp(&other.identifier))
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
    pub fn new<D: Diagnostic>(operation: &'static str, cause: D) -> Box<Self> {
        Box::new(Self {
            operation,
            element: None,
            kind: FailureKind::of::<D>(),
            cause: Arc::new(cause),
        })
    }

    pub fn new_at<I: IndexDomain, D: Diagnostic>(
        operation: &'static str,
        cause: D,
        index: &I::Index<'_>,
    ) -> Box<Self> {
        Box::new(Self {
            operation,
            element: Some(Arc::new(I::to_owned(index))),
            kind: FailureKind::of::<D>(),
            cause: Arc::new(cause),
        })
    }

    #[must_use]
    pub fn at<I: IndexDomain>(mut self: Box<Self>, index: &I::Index<'_>) -> Box<Self> {
        self.element = Some(Arc::new(I::to_owned(index)));
        self
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

impl<E: Error + Send + Sync + 'static> Diagnostic for External<E> {
    fn name() -> &'static str {
        "External"
    }
}

pub struct DuplicateIndex<I: IndexDomain> {
    index: I::Owned,
}

impl<I: IndexDomain> DuplicateIndex<I> {
    #[must_use]
    pub const fn new(index: I::Owned) -> Self {
        Self { index }
    }

    #[must_use]
    pub const fn index(&self) -> &I::Owned {
        &self.index
    }
}

impl<I: IndexDomain> Debug for DuplicateIndex<I> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DuplicateIndex")
            .field("index", &self.index)
            .finish()
    }
}

impl<I: IndexDomain> Display for DuplicateIndex<I> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "index `{}` occurs more than once in one indexed operand",
            self.index
        )
    }
}

impl<I: IndexDomain> Error for DuplicateIndex<I> {}

impl<I: IndexDomain> Diagnostic for DuplicateIndex<I> {
    fn name() -> &'static str {
        "DuplicateIndex"
    }

    fn help(&self) -> Option<String> {
        Some("construct each index at most once in one indexed operand".to_string())
    }
}

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

impl Diagnostic for GraphRecordError {
    fn name() -> &'static str {
        "GraphRecordError"
    }

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
