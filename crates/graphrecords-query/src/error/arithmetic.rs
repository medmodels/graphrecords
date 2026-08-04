use crate::Diagnostic;
use graphrecords_core::graphrecord::GraphRecordValue;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct DivisionByZero {
    dividend: GraphRecordValue,
}

impl DivisionByZero {
    #[must_use]
    pub const fn new(dividend: GraphRecordValue) -> Self {
        Self { dividend }
    }

    #[must_use]
    pub const fn dividend(&self) -> &GraphRecordValue {
        &self.dividend
    }
}

impl Display for DivisionByZero {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "cannot divide `{}` by zero", self.dividend)
    }
}

impl Error for DivisionByZero {}

impl Diagnostic for DivisionByZero {
    fn name() -> &'static str {
        "DivisionByZero"
    }
}

#[derive(Debug)]
pub struct ModuloByZero;

impl Display for ModuloByZero {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("cannot calculate a remainder with a zero modulus")
    }
}

impl Error for ModuloByZero {}

impl Diagnostic for ModuloByZero {
    fn name() -> &'static str {
        "ModuloByZero"
    }
}
