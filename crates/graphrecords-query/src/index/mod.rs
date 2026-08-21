mod edge_endpoint;
mod entity;
mod expanded;

use crate::{Failure, FailureKind, QueryResult, error::index::UnresolvedIndex};
pub use edge_endpoint::EdgeEndpointRole;
pub use entity::{EntityAttributes, GroupMembership};
pub use expanded::{
    ExpandedChild, ExpandedIndex, ExpandedIndexAddress, ExpandedIndexOwned, ExpandedIndexReference,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{
        AttributeName, AttributeNameView, EdgeAddress, EdgeIndex, GroupAddress, GroupIndex,
        GroupIndexView, Identifier, IdentifierView, NodeAddress, NodeIndex, NodeIndexView,
        StateView, Value,
    },
};
use std::{
    any::Any,
    borrow::Cow,
    fmt::{Debug, Display},
    hash::Hash,
};

pub type Position = usize;

pub trait OwnedIndex: Any + Debug + Display + Send + Sync {}

impl OwnedIndex for Position {}
impl OwnedIndex for NodeIndex {}
impl OwnedIndex for EdgeIndex {}
impl OwnedIndex for GroupIndex {}
impl OwnedIndex for AttributeName {}
impl OwnedIndex for Value {}
impl OwnedIndex for FailureKind {}
impl OwnedIndex for bool {}
impl OwnedIndex for i8 {}

pub trait IndexDomain: 'static + Clone {
    type Owned: 'static + Clone + Eq + Hash + OwnedIndex;

    type Index<'a>: Clone + Debug + Eq + Hash + 'a
    where
        Self: 'a;

    type Address: 'static + Clone + Debug + Eq + Hash + Send + Sync;

    fn index<'a>(graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a>;

    fn own_index(index: &Self::Index<'_>) -> Self::Owned;

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_>;

    fn resolve(
        graphrecord: &GraphRecord,
        owned: &Self::Owned,
        label: &'static str,
    ) -> QueryResult<Self::Address>;
}

pub trait EntityIndexDomain: IndexDomain {}

#[derive(Clone, Debug)]
pub struct Positional;

impl IndexDomain for Positional {
    type Address = Position;
    type Index<'a> = Position;
    type Owned = Position;

    fn index<'a>(_graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        *address
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        Ok(*owned)
    }
}

impl IndexDomain for NodeIndex {
    type Address = NodeAddress;
    type Index<'a> = NodeIndexView<'a>;
    type Owned = Self;

    fn index<'a>(graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        StateView::of(graphrecord).node_index(*address)
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        Self::from(index.clone())
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        NodeIndexView::from(owned)
    }

    fn resolve(
        graphrecord: &GraphRecord,
        owned: &Self::Owned,
        label: &'static str,
    ) -> QueryResult<Self::Address> {
        StateView::of(graphrecord)
            .resolve_node_address(owned)
            .ok_or_else(|| {
                Failure::new_at::<Self, _>(
                    UnresolvedIndex::<Self>::new(owned.clone()),
                    &Self::borrow_index(owned),
                    label,
                )
            })
    }
}

impl IndexDomain for EdgeIndex {
    type Address = EdgeAddress;
    type Index<'a> = Self;
    type Owned = Self;

    fn index<'a>(graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        StateView::of(graphrecord).edge_index(*address)
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }

    fn resolve(
        graphrecord: &GraphRecord,
        owned: &Self::Owned,
        label: &'static str,
    ) -> QueryResult<Self::Address> {
        StateView::of(graphrecord)
            .resolve_edge_address(owned)
            .ok_or_else(|| {
                Failure::new_at::<Self, _>(UnresolvedIndex::<Self>::new(*owned), owned, label)
            })
    }
}

impl IndexDomain for GroupIndex {
    type Address = GroupAddress;
    type Index<'a> = GroupIndexView<'a>;
    type Owned = Self;

    fn index<'a>(graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        GroupIndexView::from(StateView::of(graphrecord).group_index(*address))
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        Self::from(index.clone())
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        GroupIndexView::from(owned)
    }

    fn resolve(
        graphrecord: &GraphRecord,
        owned: &Self::Owned,
        label: &'static str,
    ) -> QueryResult<Self::Address> {
        StateView::of(graphrecord)
            .resolve_group_address(owned)
            .ok_or_else(|| {
                Failure::new_at::<Self, _>(
                    UnresolvedIndex::<Self>::new(owned.clone()),
                    &Self::borrow_index(owned),
                    label,
                )
            })
    }
}

impl IndexDomain for FailureKind {
    type Address = Self;
    type Index<'a> = Self;
    type Owned = Self;

    fn index<'a>(_graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        *address
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        Ok(*owned)
    }
}

impl IndexDomain for Value {
    type Address = Self;
    type Index<'a> = Self;
    type Owned = Self;

    fn index<'a>(_graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        address.clone()
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        index.clone()
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        owned.clone()
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        Ok(owned.clone())
    }
}

impl IndexDomain for AttributeName {
    type Address = Self;
    type Index<'a> = AttributeNameView<'a>;
    type Owned = Self;

    fn index<'a>(_graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        AttributeNameView::from(match address.identifier() {
            Identifier::Int(value) => IdentifierView::Int(*value),
            Identifier::String(value) => IdentifierView::String(Cow::Owned(value.clone())),
        })
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        Self::from(index.clone())
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        AttributeNameView::from(owned)
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        Ok(owned.clone())
    }
}

impl IndexDomain for bool {
    type Address = Self;
    type Index<'a> = Self;
    type Owned = Self;

    fn index<'a>(_graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        *address
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }

    fn resolve(
        _graphrecord: &GraphRecord,
        owned: &Self::Owned,
        _label: &'static str,
    ) -> QueryResult<Self::Address> {
        Ok(*owned)
    }
}

impl EntityIndexDomain for NodeIndex {}

impl EntityIndexDomain for EdgeIndex {}

impl EntityIndexDomain for GroupIndex {}
