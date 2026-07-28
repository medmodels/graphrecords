mod entity;
mod expanded;
mod key;

use crate::{AttributeName, FailureKind};
pub use entity::{EntityAttributes, IndicesInGroup};
pub use expanded::{
    DuplicateExpandedChildIndex, ExpandedChild, ExpandedIndex, ExpandedIndexOwned,
    ExpandedIndexReference, NoChildIndex,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex},
};
pub use key::GroupKey;
use std::{
    any::Any,
    fmt::{Debug, Display},
    hash::Hash,
};

pub type Position = usize;

pub trait OwnedIndex: Any + Debug + Display + Send + Sync {}

impl<T: Any + Debug + Display + Send + Sync> OwnedIndex for T {}

pub trait IndexDomain: 'static + Clone {
    type Owned: 'static + Clone + Eq + Hash + OwnedIndex;

    type Index<'a>: Clone + Eq + Hash
    where
        Self: 'a;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned;

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_>;
}

pub trait EntityDomain: IndexDomain {
    fn resolve_index<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Owned,
    ) -> GraphRecordResult<Self::Index<'a>>;
}

#[derive(Clone, Debug)]
pub struct Positional;

impl IndexDomain for Positional {
    type Index<'a> = Position;
    type Owned = Position;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }
}

impl IndexDomain for EdgeIndex {
    type Index<'a> = &'a Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        **index
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        owned
    }
}

impl IndexDomain for NodeIndex {
    type Index<'a> = &'a Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        (*index).clone()
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        owned
    }
}

impl IndexDomain for FailureKind {
    type Index<'a> = Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }
}

impl IndexDomain for GraphRecordValue {
    type Index<'a> = Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        index.clone()
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        owned.clone()
    }
}

impl IndexDomain for AttributeName {
    type Index<'a> = GraphRecordAttribute;
    type Owned = GraphRecordAttribute;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        index.clone()
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        owned.clone()
    }
}

impl IndexDomain for bool {
    type Index<'a> = Self;
    type Owned = Self;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        *index
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        *owned
    }
}

impl EntityDomain for EdgeIndex {
    fn resolve_index<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Owned,
    ) -> GraphRecordResult<Self::Index<'a>> {
        graphrecord.resolve_edge_index(index)
    }
}

impl EntityDomain for NodeIndex {
    fn resolve_index<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Owned,
    ) -> GraphRecordResult<Self::Index<'a>> {
        graphrecord.resolve_node_index(index)
    }
}
