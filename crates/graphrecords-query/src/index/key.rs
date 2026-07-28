use crate::{
    AttributeName, Failure, FailureKind, QueryResult,
    index::{EntityDomain, ExpandedIndex, ExpandedIndexReference, IndexDomain, Positional},
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex},
};

pub trait GroupKey: IndexDomain {
    fn resolve_key<'a>(
        label: &'static str,
        graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>>;
}

impl GroupKey for GraphRecordValue {
    fn resolve_key<'a>(
        _label: &'static str,
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(key.clone())
    }
}
impl GroupKey for bool {
    fn resolve_key<'a>(
        _label: &'static str,
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(*key)
    }
}
impl GroupKey for AttributeName {
    fn resolve_key<'a>(
        _label: &'static str,
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(key.clone())
    }
}
impl GroupKey for FailureKind {
    fn resolve_key<'a>(
        _label: &'static str,
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(*key)
    }
}
impl GroupKey for Positional {
    fn resolve_key<'a>(
        _label: &'static str,
        _graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Ok(*key)
    }
}
impl GroupKey for NodeIndex {
    fn resolve_key<'a>(
        label: &'static str,
        graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Self::resolve_index(graphrecord, key)
            .map_err(|error| Failure::new_at::<Self, _>(label, error, &Self::from_owned(key)))
    }
}
impl GroupKey for EdgeIndex {
    fn resolve_key<'a>(
        label: &'static str,
        graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        Self::resolve_index(graphrecord, key)
            .map_err(|error| Failure::new_at::<Self, _>(label, error, &Self::from_owned(key)))
    }
}
impl<P: GroupKey, C: GroupKey> GroupKey for ExpandedIndex<P, C> {
    fn resolve_key<'a>(
        label: &'static str,
        graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        let parent = P::resolve_key(label, graphrecord, key.parent_index())?;

        match key.child_index() {
            None => Ok(ExpandedIndexReference::source(parent)),
            Some(child) => Ok(ExpandedIndexReference::child(
                parent,
                C::resolve_key(label, graphrecord, child)?,
            )),
        }
    }
}
