use super::{
    Abs, Contains, EndsWith, Identifier, Lowercase, Mod, Pow, Slice, StartsWith, Trim, TrimEnd,
    TrimStart, Uppercase, Value,
};
use crate::errors::{GraphRecordError, GraphRecordResult};
#[cfg(any(feature = "serde", feature = "io"))]
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    ops::{Add, Mul, Range, Sub},
};

macro_rules! implement_identifier_wrapper {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
        #[cfg_attr(any(feature = "serde", feature = "io"), derive(Serialize, Deserialize))]
        pub struct $name(Identifier);

        impl $name {
            pub const fn identifier(&self) -> &Identifier {
                &self.0
            }
        }

        impl From<Identifier> for $name {
            fn from(identifier: Identifier) -> Self {
                Self(identifier)
            }
        }

        impl From<$name> for Identifier {
            fn from(identity: $name) -> Self {
                identity.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.into())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value.into())
            }
        }

        impl From<i64> for $name {
            fn from(value: i64) -> Self {
                Self(value.into())
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                self.0.fmt(f)
            }
        }

        impl TryFrom<Value> for $name {
            type Error = GraphRecordError;

            fn try_from(value: Value) -> Result<Self, Self::Error> {
                Identifier::try_from(value).map(Self)
            }
        }

        impl Add for $name {
            type Output = GraphRecordResult<Self>;

            fn add(self, rhs: Self) -> Self::Output {
                (self.0 + rhs.0).map(Self)
            }
        }

        impl Sub for $name {
            type Output = GraphRecordResult<Self>;

            fn sub(self, rhs: Self) -> Self::Output {
                (self.0 - rhs.0).map(Self)
            }
        }

        impl Mul for $name {
            type Output = GraphRecordResult<Self>;

            fn mul(self, rhs: Self) -> Self::Output {
                (self.0 * rhs.0).map(Self)
            }
        }

        impl Pow for $name {
            fn pow(self, exponent: Self) -> GraphRecordResult<Self> {
                self.0.pow(exponent.0).map(Self)
            }
        }

        impl Mod for $name {
            fn r#mod(self, other: Self) -> GraphRecordResult<Self> {
                self.0.r#mod(other.0).map(Self)
            }
        }

        impl Abs for $name {
            fn abs(self) -> Self {
                Self(self.0.abs())
            }
        }

        impl StartsWith for $name {
            fn starts_with(&self, other: &Self) -> bool {
                self.0.starts_with(&other.0)
            }
        }

        impl EndsWith for $name {
            fn ends_with(&self, other: &Self) -> bool {
                self.0.ends_with(&other.0)
            }
        }

        impl Contains for $name {
            fn contains(&self, other: &Self) -> bool {
                self.0.contains(&other.0)
            }
        }

        impl Slice for $name {
            fn slice(self, range: Range<usize>) -> Self {
                Self(self.0.slice(range))
            }
        }

        impl Trim for $name {
            fn trim(self) -> Self {
                Self(self.0.trim())
            }
        }

        impl TrimStart for $name {
            fn trim_start(self) -> Self {
                Self(self.0.trim_start())
            }
        }

        impl TrimEnd for $name {
            fn trim_end(self) -> Self {
                Self(self.0.trim_end())
            }
        }

        impl Lowercase for $name {
            fn lowercase(self) -> Self {
                Self(self.0.lowercase())
            }
        }

        impl Uppercase for $name {
            fn uppercase(self) -> Self {
                Self(self.0.uppercase())
            }
        }
    };
}

implement_identifier_wrapper!(NodeIndex);
implement_identifier_wrapper!(GroupIndex);
implement_identifier_wrapper!(AttributeName);
implement_identifier_wrapper!(PluginName);
