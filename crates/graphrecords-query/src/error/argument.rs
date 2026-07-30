use crate::Diagnostic;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug, Clone, Copy)]
pub enum Absent {
    Uncovered,
    Empty,
}

impl Display for Absent {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uncovered => formatter.write_str("argument did not cover this index"),
            Self::Empty => formatter.write_str("argument provided no value for this lookup"),
        }
    }
}

impl Error for Absent {}

#[derive(Debug)]
pub struct ArgumentAbsent {
    cause: Absent,
}

impl ArgumentAbsent {
    #[must_use]
    pub const fn new(cause: Absent) -> Self {
        Self { cause }
    }

    #[must_use]
    pub const fn cause(&self) -> Absent {
        self.cause
    }
}

impl Display for ArgumentAbsent {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.cause)
    }
}

impl Error for ArgumentAbsent {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}

impl Diagnostic for ArgumentAbsent {
    fn name() -> &'static str {
        "ArgumentAbsent"
    }

    fn help(&self) -> Option<String> {
        Some(
            "make the argument cover the subject's elements or state a policy with `on_missing(...)`"
                .to_string(),
        )
    }
}
