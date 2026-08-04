use crate::{Diagnostic, OwnedIndex};
use graphrecords_core::graphrecord::GraphRecordAttribute;
use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub struct MissingAttribute {
    attribute: GraphRecordAttribute,
}

impl MissingAttribute {
    #[must_use]
    pub const fn new(attribute: GraphRecordAttribute) -> Self {
        Self { attribute }
    }

    #[must_use]
    pub const fn attribute(&self) -> &GraphRecordAttribute {
        &self.attribute
    }
}

impl Display for MissingAttribute {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "no attribute `{}`", self.attribute)
    }
}

impl Error for MissingAttribute {}

impl Diagnostic for MissingAttribute {
    fn name() -> &'static str {
        "MissingAttribute"
    }

    fn help(&self) -> Option<String> {
        Some(
            "filter the elements using `has_attribute(...)` first or handle missing attributes with `on_error(...)`"
                .to_string(),
        )
    }
}

#[derive(Debug)]
pub struct MissingTraversedAttribute<T: OwnedIndex> {
    attribute: GraphRecordAttribute,
    entity: T,
}

impl<T: OwnedIndex> MissingTraversedAttribute<T> {
    #[must_use]
    pub const fn new(attribute: GraphRecordAttribute, entity: T) -> Self {
        Self { attribute, entity }
    }

    #[must_use]
    pub const fn attribute(&self) -> &GraphRecordAttribute {
        &self.attribute
    }

    #[must_use]
    pub const fn entity(&self) -> &T {
        &self.entity
    }
}

impl<T: OwnedIndex> Display for MissingTraversedAttribute<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "no attribute `{}` on the traversed element `{}`",
            self.attribute, self.entity
        )
    }
}

impl<T: OwnedIndex> Error for MissingTraversedAttribute<T> {}

impl<T: OwnedIndex> Diagnostic for MissingTraversedAttribute<T> {
    fn name() -> &'static str {
        "MissingTraversedAttribute"
    }

    fn help(&self) -> Option<String> {
        Some(
            "filter the elements using `has_attribute(...)` first or handle missing attributes with `on_error(...)`"
                .to_string(),
        )
    }
}
