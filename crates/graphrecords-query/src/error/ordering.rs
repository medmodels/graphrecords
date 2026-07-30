use crate::{Diagnostic, OwnedIndex};
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

#[derive(Debug)]
pub struct IncomparableIndices<V, E: OwnedIndex> {
    value: V,
    first: E,
    second: E,
}

impl<V, E: OwnedIndex> IncomparableIndices<V, E> {
    #[must_use]
    pub const fn new(value: V, first: E, second: E) -> Self {
        Self {
            value,
            first,
            second,
        }
    }

    #[must_use]
    pub const fn value(&self) -> &V {
        &self.value
    }

    #[must_use]
    pub const fn first(&self) -> &E {
        &self.first
    }

    #[must_use]
    pub const fn second(&self) -> &E {
        &self.second
    }
}

impl<V: Display, E: OwnedIndex> Display for IncomparableIndices<V, E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot order elements sharing value `{}`: their indices `{}` and `{}` are not comparable",
            self.value, self.first, self.second
        )
    }
}

impl<V: Debug + Display, E: OwnedIndex> Error for IncomparableIndices<V, E> {}

impl<V: Debug + Display + Send + Sync + 'static, E: OwnedIndex> Diagnostic
    for IncomparableIndices<V, E>
{
    fn name() -> &'static str {
        "IncomparableIndices"
    }

    fn help(&self) -> Option<String> {
        Some(
            "to order them deterministically, sort by a key that distinguishes these elements"
                .to_string(),
        )
    }
}
