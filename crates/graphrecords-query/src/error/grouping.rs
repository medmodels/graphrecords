use crate::{Diagnostic, Failure};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct InvalidPartitionBucketArity {
    expected: &'static str,
    actual: usize,
}

impl InvalidPartitionBucketArity {
    #[must_use]
    pub const fn new(expected: &'static str, actual: usize) -> Self {
        Self { expected, actual }
    }

    #[must_use]
    pub const fn expected(&self) -> &'static str {
        self.expected
    }

    #[must_use]
    pub const fn actual(&self) -> usize {
        self.actual
    }
}

impl Display for InvalidPartitionBucketArity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "partition bucket requires {} element(s), but received {}",
            self.expected, self.actual,
        )
    }
}

impl Error for InvalidPartitionBucketArity {}

impl Diagnostic for InvalidPartitionBucketArity {
    fn name() -> &'static str {
        "InvalidPartitionBucketArity"
    }
}

#[derive(Debug)]
pub struct MissingGroupAggregate;

impl Display for MissingGroupAggregate {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("no aggregate value for the element's group")
    }
}

impl Error for MissingGroupAggregate {}

impl Diagnostic for MissingGroupAggregate {
    fn name() -> &'static str {
        "MissingGroupAggregate"
    }

    fn help(&self) -> Option<String> {
        Some(
            "ensure every group produces a value or handle the gap with `on_error(...)`"
                .to_string(),
        )
    }
}

#[derive(Debug)]
pub struct UnresolvedGroupKeyFailures {
    failures: Vec<Failure>,
}

impl UnresolvedGroupKeyFailures {
    #[must_use]
    pub const fn new(failures: Vec<Failure>) -> Self {
        Self { failures }
    }

    #[must_use]
    pub fn failures(&self) -> &[Failure] {
        &self.failures
    }
}

impl Display for UnresolvedGroupKeyFailures {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} unresolved grouping-key failure(s) cannot be represented by this exit",
            self.failures.len(),
        )
    }
}

impl Error for UnresolvedGroupKeyFailures {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures
            .first()
            .map(|failure| failure as &(dyn Error + 'static))
    }
}

impl Diagnostic for UnresolvedGroupKeyFailures {
    fn name() -> &'static str {
        "UnresolvedGroupKeyFailures"
    }

    fn help(&self) -> Option<String> {
        Some("resolve retained key failures with `on_key_error(...)` before this exit".to_string())
    }
}

#[derive(Debug)]
pub struct UnresolvedBucketFailures {
    failures: Vec<Failure>,
}

impl UnresolvedBucketFailures {
    #[must_use]
    pub const fn new(failures: Vec<Failure>) -> Self {
        Self { failures }
    }

    #[must_use]
    pub fn failures(&self) -> &[Failure] {
        &self.failures
    }
}

impl Display for UnresolvedBucketFailures {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} unresolved bucket failure(s) cannot be represented by this exit",
            self.failures.len(),
        )
    }
}

impl Error for UnresolvedBucketFailures {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures
            .first()
            .map(|failure| failure as &(dyn Error + 'static))
    }
}

impl Diagnostic for UnresolvedBucketFailures {
    fn name() -> &'static str {
        "UnresolvedBucketFailures"
    }

    fn help(&self) -> Option<String> {
        Some(
            "resolve retained bucket failures with `on_bucket_error(...)` before this exit"
                .to_string(),
        )
    }
}
