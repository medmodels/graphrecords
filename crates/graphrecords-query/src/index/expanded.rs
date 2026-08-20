use crate::{Failure, IndexDomain, QueryResult, ValueDomain};
use graphrecords_core::GraphRecord;
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
    Parent(P),
    Child { parent: P, child: C },
}

impl<P: PartialOrd, C: PartialOrd> PartialOrd for ExpandedIndexRepresentation<P, C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let parent_ordering = match (self, other) {
            (
                Self::Parent(first)
                | Self::Child {
                    parent: first,
                    child: _,
                },
                Self::Parent(second)
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
            (Self::Parent(_), Self::Parent(_)) => Some(Ordering::Equal),
            (Self::Parent(_), Self::Child { .. }) => Some(Ordering::Less),
            (Self::Child { .. }, Self::Parent(_)) => Some(Ordering::Greater),
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
                Self::Parent(first)
                | Self::Child {
                    parent: first,
                    child: _,
                },
                Self::Parent(second)
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
            (Self::Parent(_), Self::Parent(_)) => Ordering::Equal,
            (Self::Parent(_), Self::Child { .. }) => Ordering::Less,
            (Self::Child { .. }, Self::Parent(_)) => Ordering::Greater,
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
            ExpandedIndexRepresentation::Parent(parent)
            | ExpandedIndexRepresentation::Child { parent, child: _ } => parent,
        }
    }

    #[must_use]
    pub const fn child_index(&self) -> Option<&C::Owned> {
        match &self.representation {
            ExpandedIndexRepresentation::Parent(_) => None,
            ExpandedIndexRepresentation::Child { parent: _, child } => Some(child),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (P::Owned, Option<C::Owned>) {
        match self.representation {
            ExpandedIndexRepresentation::Parent(parent) => (parent, None),
            ExpandedIndexRepresentation::Child { parent, child } => (parent, Some(child)),
        }
    }

    #[must_use]
    pub const fn is_parent(&self) -> bool {
        matches!(&self.representation, ExpandedIndexRepresentation::Parent(_))
    }

    pub(crate) const fn parent(parent: P::Owned) -> Self {
        Self {
            representation: ExpandedIndexRepresentation::Parent(parent),
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
            ExpandedIndexRepresentation::Parent(parent) => write!(formatter, "parent({parent})"),
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
            ExpandedIndexRepresentation::Parent(parent)
            | ExpandedIndexRepresentation::Child { parent, child: _ } => parent,
        }
    }

    #[must_use]
    pub const fn child_index(&self) -> Option<&C::Index<'a>> {
        match &self.representation {
            ExpandedIndexRepresentation::Parent(_) => None,
            ExpandedIndexRepresentation::Child { parent: _, child } => Some(child),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (P::Index<'a>, Option<C::Index<'a>>) {
        match self.representation {
            ExpandedIndexRepresentation::Parent(parent) => (parent, None),
            ExpandedIndexRepresentation::Child { parent, child } => (parent, Some(child)),
        }
    }

    #[must_use]
    pub const fn is_parent(&self) -> bool {
        matches!(&self.representation, ExpandedIndexRepresentation::Parent(_))
    }

    pub(crate) const fn parent(parent: P::Index<'a>) -> Self {
        Self {
            representation: ExpandedIndexRepresentation::Parent(parent),
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
            ExpandedIndexRepresentation::Parent(parent) => formatter
                .debug_tuple("Parent")
                .field(&P::own_index(parent))
                .finish(),
            ExpandedIndexRepresentation::Child { parent, child } => formatter
                .debug_struct("Child")
                .field("parent", &P::own_index(parent))
                .field("child", &C::own_index(child))
                .finish(),
        }
    }
}

pub struct ExpandedIndexAddress<P: IndexDomain, C: IndexDomain> {
    representation: ExpandedIndexRepresentation<P::Address, C::Address>,
}

impl<P: IndexDomain, C: IndexDomain> ExpandedIndexAddress<P, C> {
    #[must_use]
    pub const fn parent_index(&self) -> &P::Address {
        match &self.representation {
            ExpandedIndexRepresentation::Parent(parent)
            | ExpandedIndexRepresentation::Child { parent, child: _ } => parent,
        }
    }

    #[must_use]
    pub const fn child_index(&self) -> Option<&C::Address> {
        match &self.representation {
            ExpandedIndexRepresentation::Parent(_) => None,
            ExpandedIndexRepresentation::Child { parent: _, child } => Some(child),
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (P::Address, Option<C::Address>) {
        match self.representation {
            ExpandedIndexRepresentation::Parent(parent) => (parent, None),
            ExpandedIndexRepresentation::Child { parent, child } => (parent, Some(child)),
        }
    }

    #[must_use]
    pub const fn is_parent(&self) -> bool {
        matches!(&self.representation, ExpandedIndexRepresentation::Parent(_))
    }

    pub(crate) const fn parent(parent: P::Address) -> Self {
        Self {
            representation: ExpandedIndexRepresentation::Parent(parent),
        }
    }

    pub(crate) const fn child(parent: P::Address, child: C::Address) -> Self {
        Self {
            representation: ExpandedIndexRepresentation::Child { parent, child },
        }
    }
}

impl<P: IndexDomain, C: IndexDomain> Clone for ExpandedIndexAddress<P, C> {
    fn clone(&self) -> Self {
        Self {
            representation: self.representation.clone(),
        }
    }
}

impl<P: IndexDomain, C: IndexDomain> PartialEq for ExpandedIndexAddress<P, C> {
    fn eq(&self, other: &Self) -> bool {
        self.representation == other.representation
    }
}

impl<P: IndexDomain, C: IndexDomain> Eq for ExpandedIndexAddress<P, C> {}

impl<P: IndexDomain, C: IndexDomain> Hash for ExpandedIndexAddress<P, C> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.representation.hash(state);
    }
}

impl<P: IndexDomain, C: IndexDomain> Debug for ExpandedIndexAddress<P, C> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.representation.fmt(formatter)
    }
}

impl<P: IndexDomain, C: IndexDomain> IndexDomain for ExpandedIndex<P, C> {
    type Address = ExpandedIndexAddress<P, C>;
    type Index<'a>
        = ExpandedIndexReference<'a, P, C>
    where
        Self: 'a;
    type Owned = ExpandedIndexOwned<P, C>;

    fn index<'a>(graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        match &address.representation {
            ExpandedIndexRepresentation::Parent(parent) => {
                ExpandedIndexReference::parent(P::index(graphrecord, parent))
            }
            ExpandedIndexRepresentation::Child { parent, child } => ExpandedIndexReference::child(
                P::index(graphrecord, parent),
                C::index(graphrecord, child),
            ),
        }
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        match &index.representation {
            ExpandedIndexRepresentation::Parent(parent) => {
                ExpandedIndexOwned::parent(P::own_index(parent))
            }
            ExpandedIndexRepresentation::Child { parent, child } => {
                ExpandedIndexOwned::child(P::own_index(parent), C::own_index(child))
            }
        }
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        match &owned.representation {
            ExpandedIndexRepresentation::Parent(parent) => {
                ExpandedIndexReference::parent(P::borrow_index(parent))
            }
            ExpandedIndexRepresentation::Child { parent, child } => {
                ExpandedIndexReference::child(P::borrow_index(parent), C::borrow_index(child))
            }
        }
    }

    fn resolve(
        graphrecord: &GraphRecord,
        owned: &Self::Owned,
        label: &'static str,
    ) -> QueryResult<Self::Address> {
        match &owned.representation {
            ExpandedIndexRepresentation::Parent(parent) => Ok(ExpandedIndexAddress::parent(
                P::resolve(graphrecord, parent, label)?,
            )),
            ExpandedIndexRepresentation::Child { parent, child } => {
                Ok(ExpandedIndexAddress::child(
                    P::resolve(graphrecord, parent, label)?,
                    C::resolve(graphrecord, child, label)?,
                ))
            }
        }
    }
}

pub struct ExpandedChild<'a, C: IndexDomain, V: ValueDomain> {
    address: C::Address,
    outcome: QueryResult<V::Value<'a>>,
}

impl<'a, C: IndexDomain, V: ValueDomain> ExpandedChild<'a, C, V> {
    #[must_use]
    pub const fn success(address: C::Address, value: V::Value<'a>) -> Self {
        Self {
            address,
            outcome: Ok(value),
        }
    }

    #[must_use]
    pub const fn failure(address: C::Address, failure: Box<Failure>) -> Self {
        Self {
            address,
            outcome: Err(failure),
        }
    }

    #[must_use]
    pub const fn from_outcome(address: C::Address, outcome: QueryResult<V::Value<'a>>) -> Self {
        Self { address, outcome }
    }

    pub(crate) fn into_parts(self) -> (C::Address, QueryResult<V::Value<'a>>) {
        (self.address, self.outcome)
    }
}
