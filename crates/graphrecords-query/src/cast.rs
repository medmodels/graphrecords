use std::{
    fmt::{self, Display, Formatter},
    hash::Hash,
};

pub trait CastTarget: Clone + Display + Eq + Hash + 'static {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Bool;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DateTime;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Float;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Int;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct String;

impl CastTarget for Bool {}

impl CastTarget for DateTime {}

impl CastTarget for Duration {}

impl CastTarget for Float {}

impl CastTarget for Int {}

impl CastTarget for String {}

impl Display for Bool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("Bool")
    }
}

impl Display for DateTime {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("DateTime")
    }
}

impl Display for Duration {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("Duration")
    }
}

impl Display for Float {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("Float")
    }
}

impl Display for Int {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("Int")
    }
}

impl Display for String {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("String")
    }
}
