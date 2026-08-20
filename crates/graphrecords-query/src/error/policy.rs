use crate::{Diagnostic, Failure};
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct RaisedFailures {
    failures: Vec<Failure>,
}

impl RaisedFailures {
    #[must_use]
    pub const fn new(failures: Vec<Failure>) -> Self {
        Self { failures }
    }

    #[must_use]
    pub fn failures(&self) -> &[Failure] {
        &self.failures
    }
}

impl Display for RaisedFailures {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} failing element(s)", self.failures.len())?;

        for failure in &self.failures {
            write!(formatter, "\n{failure}")?;
        }

        Ok(())
    }
}

impl Error for RaisedFailures {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures
            .first()
            .map(|failure| failure as &(dyn Error + 'static))
    }
}

impl Diagnostic for RaisedFailures {
    fn name() -> &'static str {
        "RaisedFailures"
    }

    fn help(&self) -> Option<String> {
        Some(
            "drop the failing elements with `on_error(Drop)` or replace them with `on_error(Replace(...))`"
                .to_string(),
        )
    }
}
