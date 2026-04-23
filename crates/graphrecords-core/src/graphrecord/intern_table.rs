use super::graph::{Attributes, NodeIndex};
use super::group_mapping::Group;
use super::{GraphRecordAttribute, GraphRecordValue};
use graphrecords_utils::aliases::GrHashMap;
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

#[cfg(feature = "safe-handles")]
pub fn fresh_graph_id() -> u32 {
    rand::random::<u32>()
}

pub trait HandleKind: 'static {
    type Value: Clone + Eq + Hash + fmt::Debug + fmt::Display;
}

#[derive(Debug)]
pub enum NodeIndexKind {}
impl HandleKind for NodeIndexKind {
    type Value = NodeIndex;
}

#[derive(Debug)]
pub enum AttributeNameKind {}
impl HandleKind for AttributeNameKind {
    type Value = GraphRecordAttribute;
}

#[derive(Debug)]
pub enum GroupKind {}
impl HandleKind for GroupKind {
    type Value = Group;
}

#[cfg_attr(not(feature = "safe-handles"), repr(transparent))]
pub struct Handle<K: HandleKind> {
    #[cfg(feature = "safe-handles")]
    graph_id: u32,
    index: u32,
    _marker: PhantomData<fn() -> K>,
}

impl<K: HandleKind> Handle<K> {
    #[cfg(not(feature = "safe-handles"))]
    pub(crate) const fn new(index: u32) -> Self {
        Self {
            index,
            _marker: PhantomData,
        }
    }

    #[cfg(feature = "safe-handles")]
    pub(crate) const fn new(graph_id: u32, index: u32) -> Self {
        Self {
            graph_id,
            index,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) const fn index(self) -> u32 {
        self.index
    }

    #[cfg(feature = "safe-handles")]
    #[inline]
    #[must_use]
    pub const fn graph_id(self) -> u32 {
        self.graph_id
    }
}

impl<K: HandleKind> Clone for Handle<K> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: HandleKind> Copy for Handle<K> {}

impl<K: HandleKind> PartialEq for Handle<K> {
    #[cfg(not(feature = "safe-handles"))]
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }

    #[cfg(feature = "safe-handles")]
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.graph_id == other.graph_id && self.index == other.index
    }
}

impl<K: HandleKind> Eq for Handle<K> {}

impl<K: HandleKind> Hash for Handle<K> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.index.hash(state);
    }
}

impl<K: HandleKind> PartialOrd for Handle<K> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: HandleKind> Ord for Handle<K> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.index.cmp(&other.index)
    }
}

impl<K: HandleKind> fmt::Debug for Handle<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = std::any::type_name::<K>()
            .rsplit("::")
            .next()
            .unwrap_or("Handle");
        write!(f, "Handle<{kind}>({})", self.index)
    }
}

#[cfg(all(feature = "serde", not(feature = "safe-handles")))]
impl<K: HandleKind> Serialize for Handle<K> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.index.serialize(serializer)
    }
}

#[cfg(all(feature = "serde", feature = "safe-handles"))]
impl<K: HandleKind> Serialize for Handle<K> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.graph_id, self.index).serialize(serializer)
    }
}

#[cfg(all(feature = "serde", not(feature = "safe-handles")))]
impl<'de, K: HandleKind> Deserialize<'de> for Handle<K> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let index = u32::deserialize(deserializer)?;
        Ok(Self::new(index))
    }
}

#[cfg(all(feature = "serde", feature = "safe-handles"))]
impl<'de, K: HandleKind> Deserialize<'de> for Handle<K> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (graph_id, index) = <(u32, u32)>::deserialize(deserializer)?;
        Ok(Self::new(graph_id, index))
    }
}

pub type NodeHandle = Handle<NodeIndexKind>;
pub type AttributeHandle = Handle<AttributeNameKind>;
pub type GroupHandle = Handle<GroupKind>;

pub struct InternTable<K: HandleKind> {
    values: Vec<Arc<K::Value>>,
    lookup: GrHashMap<Arc<K::Value>, Handle<K>>,
    #[cfg(feature = "safe-handles")]
    graph_id: u32,
}

impl<K: HandleKind> InternTable<K> {
    #[cfg(not(feature = "safe-handles"))]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            lookup: GrHashMap::with_capacity(capacity),
        }
    }

    #[cfg(feature = "safe-handles")]
    pub(crate) fn with_capacity(capacity: usize, graph_id: u32) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            lookup: GrHashMap::with_capacity(capacity),
            graph_id,
        }
    }

    #[cfg(feature = "safe-handles")]
    pub(crate) fn set_graph_id(&mut self, graph_id: u32) {
        self.graph_id = graph_id;

        for handle in self.lookup.values_mut() {
            *handle = Handle::new(graph_id, handle.index);
        }
    }

    #[cfg(not(feature = "safe-handles"))]
    #[allow(clippy::unused_self)]
    const fn make_handle(&self, index: u32) -> Handle<K> {
        Handle::new(index)
    }

    #[cfg(feature = "safe-handles")]
    const fn make_handle(&self, index: u32) -> Handle<K> {
        Handle::new(self.graph_id, index)
    }

    pub(crate) fn intern_owned(&mut self, value: K::Value) -> Handle<K> {
        if let Some(&handle) = self.lookup.get(&value) {
            return handle;
        }

        let index = u32::try_from(self.values.len())
            .expect("InternTable overflow: more than u32::MAX unique entries");
        let handle = self.make_handle(index);
        let arc = Arc::new(value);

        self.values.push(Arc::clone(&arc));
        self.lookup.insert(arc, handle);

        handle
    }

    pub(crate) fn get(&self, value: &K::Value) -> Option<Handle<K>> {
        self.lookup.get(value).copied()
    }

    pub(crate) fn resolve(&self, handle: Handle<K>) -> &K::Value {
        &self.values[handle.index as usize]
    }

    pub(crate) const fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn clear(&mut self) {
        self.values.clear();
        self.lookup.clear();
    }
}

impl<K: HandleKind> Default for InternTable<K> {
    #[cfg(not(feature = "safe-handles"))]
    fn default() -> Self {
        Self::with_capacity(0)
    }

    #[cfg(feature = "safe-handles")]
    fn default() -> Self {
        Self::with_capacity(0, 0)
    }
}

impl<K: HandleKind> Clone for InternTable<K> {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            lookup: self.lookup.clone(),
            #[cfg(feature = "safe-handles")]
            graph_id: self.graph_id,
        }
    }
}

impl<K: HandleKind> fmt::Debug for InternTable<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InternTable")
            .field("len", &self.values.len())
            .finish_non_exhaustive()
    }
}

#[cfg(all(feature = "serde", not(feature = "safe-handles")))]
impl<K: HandleKind> Serialize for InternTable<K>
where
    K::Value: Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let values: Vec<&K::Value> = self
            .values
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();
        values.serialize(serializer)
    }
}

#[cfg(all(feature = "serde", feature = "safe-handles"))]
impl<K: HandleKind> Serialize for InternTable<K>
where
    K::Value: Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let values: Vec<&K::Value> = self
            .values
            .iter()
            .map(std::convert::AsRef::as_ref)
            .collect();
        (self.graph_id, values).serialize(serializer)
    }
}

#[cfg(all(feature = "serde", not(feature = "safe-handles")))]
impl<'de, K: HandleKind> Deserialize<'de> for InternTable<K>
where
    K::Value: Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let values: Vec<K::Value> = Vec::deserialize(deserializer)?;
        let mut table = Self::with_capacity(values.len());
        for value in values {
            table.intern_owned(value);
        }
        Ok(table)
    }
}

#[cfg(all(feature = "serde", feature = "safe-handles"))]
impl<'de, K: HandleKind> Deserialize<'de> for InternTable<K>
where
    K::Value: Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (graph_id, values): (u32, Vec<K::Value>) =
            <(u32, Vec<K::Value>)>::deserialize(deserializer)?;
        let mut table = Self::with_capacity(values.len(), graph_id);
        for value in values {
            table.intern_owned(value);
        }
        Ok(table)
    }
}

pub type HandleAttributes = HashMap<AttributeHandle, GraphRecordValue>;

pub struct AttributesView<'a> {
    table: &'a InternTable<AttributeNameKind>,
    map: &'a HandleAttributes,
}

impl<'a> AttributesView<'a> {
    pub(crate) const fn new(
        table: &'a InternTable<AttributeNameKind>,
        map: &'a HandleAttributes,
    ) -> Self {
        Self { table, map }
    }

    #[must_use]
    pub fn get(&self, name: &GraphRecordAttribute) -> Option<&'a GraphRecordValue> {
        let handle = self.table.get(name)?;
        self.map.get(&handle)
    }

    #[must_use]
    pub fn contains_key(&self, name: &GraphRecordAttribute) -> bool {
        self.table
            .get(name)
            .is_some_and(|handle| self.map.contains_key(&handle))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&'a GraphRecordAttribute, &'a GraphRecordValue)> + '_ {
        self.map
            .iter()
            .map(move |(handle, value)| (self.table.resolve(*handle), value))
    }

    pub fn keys(&self) -> impl Iterator<Item = &'a GraphRecordAttribute> + '_ {
        self.map
            .keys()
            .map(move |handle| self.table.resolve(*handle))
    }

    pub fn values(&self) -> impl Iterator<Item = &'a GraphRecordValue> + '_ {
        self.map.values()
    }

    #[must_use]
    pub fn to_owned_attributes(&self) -> Attributes {
        self.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

impl fmt::Debug for AttributesView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl PartialEq for AttributesView<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.iter()
            .all(|(name, value)| other.get(name) == Some(value))
    }
}

impl PartialEq<Attributes> for AttributesView<'_> {
    fn eq(&self, other: &Attributes) -> bool {
        if self.len() != other.len() {
            return false;
        }
        other
            .iter()
            .all(|(name, value)| self.get(name) == Some(value))
    }
}

impl PartialEq<&Attributes> for AttributesView<'_> {
    fn eq(&self, other: &&Attributes) -> bool {
        self == *other
    }
}

impl<'a> PartialEq<AttributesView<'a>> for Attributes {
    fn eq(&self, other: &AttributesView<'a>) -> bool {
        other == self
    }
}

impl<'a> PartialEq<AttributesView<'a>> for &Attributes {
    fn eq(&self, other: &AttributesView<'a>) -> bool {
        other == *self
    }
}

pub trait AttributesMap {
    fn contains_attribute(&self, name: &GraphRecordAttribute) -> bool;
    fn iter_attributes(
        &self,
    ) -> impl Iterator<Item = (&GraphRecordAttribute, &GraphRecordValue)> + '_;
    fn attribute_count(&self) -> usize;
    fn has_attributes(&self) -> bool {
        self.attribute_count() > 0
    }
}

impl AttributesMap for Attributes {
    fn contains_attribute(&self, name: &GraphRecordAttribute) -> bool {
        self.contains_key(name)
    }

    fn iter_attributes(
        &self,
    ) -> impl Iterator<Item = (&GraphRecordAttribute, &GraphRecordValue)> + '_ {
        self.iter()
    }

    fn attribute_count(&self) -> usize {
        self.len()
    }
}

impl AttributesMap for AttributesView<'_> {
    fn contains_attribute(&self, name: &GraphRecordAttribute) -> bool {
        self.contains_key(name)
    }

    fn iter_attributes(
        &self,
    ) -> impl Iterator<Item = (&GraphRecordAttribute, &GraphRecordValue)> + '_ {
        self.iter()
    }

    fn attribute_count(&self) -> usize {
        self.len()
    }
}

impl<T: AttributesMap + ?Sized> AttributesMap for &T {
    fn contains_attribute(&self, name: &GraphRecordAttribute) -> bool {
        (*self).contains_attribute(name)
    }

    fn iter_attributes(
        &self,
    ) -> impl Iterator<Item = (&GraphRecordAttribute, &GraphRecordValue)> + '_ {
        (*self).iter_attributes()
    }

    fn attribute_count(&self) -> usize {
        (*self).attribute_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_dedups_same_value() {
        let mut table: InternTable<AttributeNameKind> = InternTable::default();
        let a = table.intern_owned(GraphRecordAttribute::from("name"));
        let b = table.intern_owned(GraphRecordAttribute::from("name"));
        assert_eq!(a, b);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn intern_distinguishes_int_from_string() {
        let mut table: InternTable<NodeIndexKind> = InternTable::default();
        let as_int = table.intern_owned(GraphRecordAttribute::from(42_i64));
        let as_string = table.intern_owned(GraphRecordAttribute::from("42"));
        assert_ne!(as_int, as_string);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn intern_assigns_sequential_indices() {
        let mut table: InternTable<AttributeNameKind> = InternTable::default();
        let a = table.intern_owned(GraphRecordAttribute::from("a"));
        let b = table.intern_owned(GraphRecordAttribute::from("b"));
        let c = table.intern_owned(GraphRecordAttribute::from("c"));
        assert_eq!(a.index(), 0);
        assert_eq!(b.index(), 1);
        assert_eq!(c.index(), 2);
    }

    #[test]
    fn get_does_not_insert_on_miss() {
        let table: InternTable<AttributeNameKind> = InternTable::default();
        let missing = table.get(&GraphRecordAttribute::from("never_inserted"));
        assert!(missing.is_none());
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn get_returns_existing_handle() {
        let mut table: InternTable<AttributeNameKind> = InternTable::default();
        let inserted = table.intern_owned(GraphRecordAttribute::from("present"));
        let found = table.get(&GraphRecordAttribute::from("present"));
        assert_eq!(found, Some(inserted));
    }

    #[test]
    fn resolve_roundtrips() {
        let mut table: InternTable<AttributeNameKind> = InternTable::default();
        let original = GraphRecordAttribute::from("round-trip");
        let handle = table.intern_owned(original.clone());
        assert_eq!(table.resolve(handle), &original);
    }

    #[test]
    fn resolve_int_variant_roundtrips() {
        let mut table: InternTable<NodeIndexKind> = InternTable::default();
        let original = GraphRecordAttribute::from(1234_i64);
        let handle = table.intern_owned(original.clone());
        assert_eq!(table.resolve(handle), &original);
    }

    #[test]
    fn clear_empties_table() {
        let mut table: InternTable<AttributeNameKind> = InternTable::default();
        table.intern_owned(GraphRecordAttribute::from("a"));
        table.intern_owned(GraphRecordAttribute::from("b"));
        assert_eq!(table.len(), 2);
        table.clear();
        assert_eq!(table.len(), 0);
        let reinserted = table.intern_owned(GraphRecordAttribute::from("a"));
        assert_eq!(reinserted.index(), 0);
    }

    #[cfg(not(feature = "safe-handles"))]
    #[test]
    fn handles_are_four_bytes() {
        assert_eq!(std::mem::size_of::<NodeHandle>(), 4);
        assert_eq!(std::mem::size_of::<AttributeHandle>(), 4);
        assert_eq!(std::mem::size_of::<GroupHandle>(), 4);
    }

    #[cfg(feature = "safe-handles")]
    #[test]
    fn handles_are_eight_bytes_under_safe_handles() {
        assert_eq!(std::mem::size_of::<NodeHandle>(), 8);
        assert_eq!(std::mem::size_of::<AttributeHandle>(), 8);
        assert_eq!(std::mem::size_of::<GroupHandle>(), 8);
    }

    #[cfg(feature = "safe-handles")]
    #[test]
    fn handle_graph_id_round_trips() {
        let handle = NodeHandle::new(7, 42);
        assert_eq!(handle.graph_id(), 7);
        assert_eq!(handle.index(), 42);
    }

    #[cfg(feature = "safe-handles")]
    #[test]
    fn handles_from_different_graph_ids_are_not_equal() {
        let handle_a = NodeHandle::new(1, 5);
        let handle_b = NodeHandle::new(2, 5);
        assert_ne!(handle_a, handle_b);
    }

    #[cfg(feature = "safe-handles")]
    #[test]
    fn fresh_graph_id_returns_distinct_values() {
        let mut ids = std::collections::HashSet::new();
        for _ in 0..16 {
            ids.insert(fresh_graph_id());
        }
        assert!(ids.len() >= 15);
    }

    #[cfg(feature = "safe-handles")]
    #[test]
    fn set_graph_id_restamps_existing_lookup_handles() {
        let mut table: InternTable<NodeIndexKind> = InternTable::with_capacity(0, 1);

        table.intern_owned(GraphRecordAttribute::from("a"));
        table.intern_owned(GraphRecordAttribute::from("b"));

        table.set_graph_id(5);

        let handle = table.get(&GraphRecordAttribute::from("a")).unwrap();

        assert_eq!(handle.graph_id(), 5);
    }
}
