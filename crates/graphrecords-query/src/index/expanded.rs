use crate::{Failure, IndexDomain, QueryResult, ValueDomain};
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Display, Formatter},
    hash::{Hash, Hasher},
    marker::PhantomData,
};

#[derive(Clone)]
pub struct ExpandedIndex<P: IndexDomain, C: IndexDomain>(PhantomData<(P, C)>);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ExpandedIndexRepresentation<P, C> {
    Source(P),
    Child { parent: P, child: C },
}

impl<P: PartialOrd, C: PartialOrd> PartialOrd for ExpandedIndexRepresentation<P, C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let parent_ordering = match (self, other) {
            (
                Self::Source(first)
                | Self::Child {
                    parent: first,
                    child: _,
                },
                Self::Source(second)
                | Self::Child {
                    parent: second,
                    child: _,
                },
            ) => first.partial_cmp(second)?,
        };

        if parent_ordering != Ordering::Equal {
            return Some(parent_ordering);
        }

        match (self, other) {
            (Self::Source(_), Self::Source(_)) => Some(Ordering::Equal),
            (Self::Source(_), Self::Child { .. }) => Some(Ordering::Less),
            (Self::Child { .. }, Self::Source(_)) => Some(Ordering::Greater),
            (
                Self::Child {
                    parent: _,
                    child: first,
                },
                Self::Child {
                    parent: _,
                    child: second,
                },
            ) => first.partial_cmp(second),
        }
    }
}

impl<P: Ord, C: Ord> Ord for ExpandedIndexRepresentation<P, C> {
    fn cmp(&self, other: &Self) -> Ordering {
        let parent_ordering = match (self, other) {
            (
                Self::Source(first)
                | Self::Child {
                    parent: first,
                    child: _,
                },
                Self::Source(second)
                | Self::Child {
                    parent: second,
                    child: _,
                },
            ) => first.cmp(second),
        };

        if parent_ordering != Ordering::Equal {
            return parent_ordering;
        }

        match (self, other) {
            (Self::Source(_), Self::Source(_)) => Ordering::Equal,
            (Self::Source(_), Self::Child { .. }) => Ordering::Less,
            (Self::Child { .. }, Self::Source(_)) => Ordering::Greater,
            (
                Self::Child {
                    parent: _,
                    child: first,
                },
                Self::Child {
                    parent: _,
                    child: second,
                },
            ) => first.cmp(second),
        }
    }
}

pub struct ExpandedIndexOwned<P: IndexDomain, C: IndexDomain> {
    representation: ExpandedIndexRepresentation<P::Owned, C::Owned>,
}

impl<P: IndexDomain, C: IndexDomain> ExpandedIndexOwned<P, C> {
    #[must_use]
    pub const fn parent_index(&self) -> &P::Owned {
        match &self.representation {
            ExpandedIndexRepresentation::Source(parent)
            | ExpandedIndexRepresentation::Child { parent, child: _ } => parent,
        }
    }

    #[must_use]
    pub const fn child_index(&self) -> Option<&C::Owned> {
        match &self.representation {
            ExpandedIndexRepresentation::Source(_) => None,
            ExpandedIndexRepresentation::Child { parent: _, child } => Some(child),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (P::Owned, Option<C::Owned>) {
        match self.representation {
            ExpandedIndexRepresentation::Source(parent) => (parent, None),
            ExpandedIndexRepresentation::Child { parent, child } => (parent, Some(child)),
        }
    }

    #[must_use]
    pub const fn is_source(&self) -> bool {
        matches!(&self.representation, ExpandedIndexRepresentation::Source(_))
    }

    pub(crate) const fn source(parent: P::Owned) -> Self {
        Self {
            representation: ExpandedIndexRepresentation::Source(parent),
        }
    }

    pub(crate) const fn child(parent: P::Owned, child: C::Owned) -> Self {
        Self {
            representation: ExpandedIndexRepresentation::Child { parent, child },
        }
    }
}

impl<P: IndexDomain, C: IndexDomain> Clone for ExpandedIndexOwned<P, C> {
    fn clone(&self) -> Self {
        Self {
            representation: self.representation.clone(),
        }
    }
}

impl<P: IndexDomain, C: IndexDomain> PartialEq for ExpandedIndexOwned<P, C> {
    fn eq(&self, other: &Self) -> bool {
        self.representation == other.representation
    }
}

impl<P: IndexDomain, C: IndexDomain> Eq for ExpandedIndexOwned<P, C> {}

impl<P, C> PartialOrd for ExpandedIndexOwned<P, C>
where
    P: IndexDomain,
    C: IndexDomain,
    P::Owned: PartialOrd,
    C::Owned: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.representation.partial_cmp(&other.representation)
    }
}

impl<P, C> Ord for ExpandedIndexOwned<P, C>
where
    P: IndexDomain,
    C: IndexDomain,
    P::Owned: Ord,
    C::Owned: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.representation.cmp(&other.representation)
    }
}

impl<P: IndexDomain, C: IndexDomain> Hash for ExpandedIndexOwned<P, C> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.representation.hash(state);
    }
}

impl<P: IndexDomain, C: IndexDomain> Debug for ExpandedIndexOwned<P, C> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.representation.fmt(formatter)
    }
}

impl<P: IndexDomain, C: IndexDomain> Display for ExpandedIndexOwned<P, C> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.representation {
            ExpandedIndexRepresentation::Source(parent) => write!(formatter, "source({parent})"),
            ExpandedIndexRepresentation::Child { parent, child } => {
                write!(formatter, "child({parent}, {child})")
            }
        }
    }
}

pub struct ExpandedIndexReference<'a, P: IndexDomain, C: IndexDomain> {
    representation: ExpandedIndexRepresentation<P::Index<'a>, C::Index<'a>>,
}

impl<'a, P: IndexDomain, C: IndexDomain> ExpandedIndexReference<'a, P, C> {
    #[must_use]
    pub const fn parent_index(&self) -> &P::Index<'a> {
        match &self.representation {
            ExpandedIndexRepresentation::Source(parent)
            | ExpandedIndexRepresentation::Child { parent, child: _ } => parent,
        }
    }

    #[must_use]
    pub const fn child_index(&self) -> Option<&C::Index<'a>> {
        match &self.representation {
            ExpandedIndexRepresentation::Source(_) => None,
            ExpandedIndexRepresentation::Child { parent: _, child } => Some(child),
        }
    }

    #[must_use]
    pub const fn is_source(&self) -> bool {
        matches!(&self.representation, ExpandedIndexRepresentation::Source(_))
    }

    pub(crate) const fn source(parent: P::Index<'a>) -> Self {
        Self {
            representation: ExpandedIndexRepresentation::Source(parent),
        }
    }

    pub(crate) const fn child(parent: P::Index<'a>, child: C::Index<'a>) -> Self {
        Self {
            representation: ExpandedIndexRepresentation::Child { parent, child },
        }
    }
}

impl<P: IndexDomain, C: IndexDomain> Clone for ExpandedIndexReference<'_, P, C> {
    fn clone(&self) -> Self {
        Self {
            representation: self.representation.clone(),
        }
    }
}

impl<P: IndexDomain, C: IndexDomain> PartialEq for ExpandedIndexReference<'_, P, C> {
    fn eq(&self, other: &Self) -> bool {
        self.representation == other.representation
    }
}

impl<P: IndexDomain, C: IndexDomain> Eq for ExpandedIndexReference<'_, P, C> {}

impl<'a, P, C> PartialOrd for ExpandedIndexReference<'a, P, C>
where
    P: IndexDomain,
    C: IndexDomain,
    P::Index<'a>: PartialOrd,
    C::Index<'a>: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.representation.partial_cmp(&other.representation)
    }
}

impl<'a, P, C> Ord for ExpandedIndexReference<'a, P, C>
where
    P: IndexDomain,
    C: IndexDomain,
    P::Index<'a>: Ord,
    C::Index<'a>: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.representation.cmp(&other.representation)
    }
}

impl<P: IndexDomain, C: IndexDomain> Hash for ExpandedIndexReference<'_, P, C> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.representation.hash(state);
    }
}

impl<P: IndexDomain, C: IndexDomain> Debug for ExpandedIndexReference<'_, P, C> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.representation {
            ExpandedIndexRepresentation::Source(parent) => formatter
                .debug_tuple("Source")
                .field(&P::to_owned(parent))
                .finish(),
            ExpandedIndexRepresentation::Child { parent, child } => formatter
                .debug_struct("Child")
                .field("parent", &P::to_owned(parent))
                .field("child", &C::to_owned(child))
                .finish(),
        }
    }
}

impl<P: IndexDomain, C: IndexDomain> IndexDomain for ExpandedIndex<P, C> {
    type Index<'a>
        = ExpandedIndexReference<'a, P, C>
    where
        Self: 'a;
    type Owned = ExpandedIndexOwned<P, C>;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        match &index.representation {
            ExpandedIndexRepresentation::Source(parent) => {
                ExpandedIndexOwned::source(P::to_owned(parent))
            }
            ExpandedIndexRepresentation::Child { parent, child } => {
                ExpandedIndexOwned::child(P::to_owned(parent), C::to_owned(child))
            }
        }
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        match &owned.representation {
            ExpandedIndexRepresentation::Source(parent) => {
                ExpandedIndexReference::source(P::from_owned(parent))
            }
            ExpandedIndexRepresentation::Child { parent, child } => {
                ExpandedIndexReference::child(P::from_owned(parent), C::from_owned(child))
            }
        }
    }
}

pub struct ExpandedChild<'a, C: IndexDomain, V: ValueDomain> {
    index: C::Index<'a>,
    outcome: QueryResult<V::Value<'a>>,
}

impl<'a, C: IndexDomain, V: ValueDomain> ExpandedChild<'a, C, V> {
    #[must_use]
    pub const fn success(index: C::Index<'a>, value: V::Value<'a>) -> Self {
        Self {
            index,
            outcome: Ok(value),
        }
    }

    #[must_use]
    pub const fn failure(index: C::Index<'a>, failure: Box<Failure>) -> Self {
        Self {
            index,
            outcome: Err(failure),
        }
    }

    #[must_use]
    pub const fn from_outcome(index: C::Index<'a>, outcome: QueryResult<V::Value<'a>>) -> Self {
        Self { index, outcome }
    }

    pub(crate) fn into_parts(self) -> (C::Index<'a>, QueryResult<V::Value<'a>>) {
        (self.index, self.outcome)
    }
}
