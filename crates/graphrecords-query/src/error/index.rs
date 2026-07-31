use crate::{Diagnostic, IndexDomain};
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

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

pub struct DuplicateExpandedChildIndex<C: IndexDomain> {
    index: C::Owned,
}

impl<C: IndexDomain> DuplicateExpandedChildIndex<C> {
    #[must_use]
    pub const fn new(index: C::Owned) -> Self {
        Self { index }
    }

    #[must_use]
    pub const fn index(&self) -> &C::Owned {
        &self.index
    }
}

impl<C: IndexDomain> Debug for DuplicateExpandedChildIndex<C> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DuplicateExpandedChildIndex")
            .field("index", &self.index)
            .finish()
    }
}

impl<C: IndexDomain> Display for DuplicateExpandedChildIndex<C> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "expanded child index `{}` occurs more than once under one parent",
            self.index
        )
    }
}

impl<C: IndexDomain> Error for DuplicateExpandedChildIndex<C> {}

impl<C: IndexDomain> Diagnostic for DuplicateExpandedChildIndex<C> {
    fn name() -> &'static str {
        "DuplicateExpandedChildIndex"
    }

    fn help(&self) -> Option<String> {
        Some("emit each child index at most once for one expanded parent".to_string())
    }
}

pub struct NoChildIndex<P: IndexDomain> {
    parent: P::Owned,
}

impl<P: IndexDomain> NoChildIndex<P> {
    #[must_use]
    pub const fn new(parent: P::Owned) -> Self {
        Self { parent }
    }

    #[must_use]
    pub const fn parent_index(&self) -> &P::Owned {
        &self.parent
    }
}

impl<P: IndexDomain> Debug for NoChildIndex<P> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NoChildIndex")
            .field("parent", &self.parent)
            .finish()
    }
}

impl<P: IndexDomain> Display for NoChildIndex<P> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "address `source({})` has no child component",
            self.parent
        )
    }
}

impl<P: IndexDomain> Error for NoChildIndex<P> {}

impl<P: IndexDomain> Diagnostic for NoChildIndex<P> {
    fn name() -> &'static str {
        "NoChildIndex"
    }

    fn help(&self) -> Option<String> {
        Some(
            "source(...) addresses mark parents whose expansion failed; handle those elements with on_error before projecting child indices"
                .to_string(),
        )
    }
}
