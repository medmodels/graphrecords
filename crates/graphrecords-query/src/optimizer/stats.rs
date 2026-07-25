use graphrecords_core::{
    GraphRecord,
    graphrecord::{GraphRecordAttribute, Group},
};
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    hash::Hash,
};

pub trait Statistic: 'static {
    type Key: Hash + Eq + Clone + 'static;
    type Value: Clone + 'static;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value;
}

pub struct Stats<'a> {
    graphrecord: &'a GraphRecord,
    cache: RefCell<GrHashMap<TypeId, Box<dyn Any>>>,
}

impl<'a> Stats<'a> {
    #[must_use]
    pub fn new(graphrecord: &'a GraphRecord) -> Self {
        Self {
            graphrecord,
            cache: RefCell::new(GrHashMap::default()),
        }
    }

    pub fn get<S: Statistic>(&self, key: &S::Key) -> S::Value {
        let mut cache = self.cache.borrow_mut();

        #[allow(clippy::missing_panics_doc)]
        let map = cache
            .entry(TypeId::of::<S>())
            .or_insert_with(|| Box::new(GrHashMap::<S::Key, S::Value>::default()))
            .downcast_mut::<GrHashMap<S::Key, S::Value>>()
            .expect("statistic cache type always matches its TypeId key");

        map.entry(key.clone())
            .or_insert_with(|| S::compute(self.graphrecord, key))
            .clone()
    }
}

pub struct NodeGroupSize;

impl Statistic for NodeGroupSize {
    type Key = Group;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value {
        graphrecord.nodes_in_group(key).map_or(0, Iterator::count)
    }
}

pub struct EdgeGroupSize;

impl Statistic for EdgeGroupSize {
    type Key = Group;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value {
        graphrecord.edges_in_group(key).map_or(0, Iterator::count)
    }
}

pub struct NodeAttributeCardinality;

impl Statistic for NodeAttributeCardinality {
    type Key = GraphRecordAttribute;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value {
        graphrecord
            .node_indices()
            .filter_map(|node_index| {
                graphrecord
                    .node_attributes(node_index)
                    .ok()?
                    .get(key)
                    .cloned()
            })
            .collect::<GrHashSet<_>>()
            .len()
    }
}

pub struct EdgeAttributeCardinality;

impl Statistic for EdgeAttributeCardinality {
    type Key = GraphRecordAttribute;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value {
        graphrecord
            .edge_indices()
            .filter_map(|edge_index| {
                graphrecord
                    .edge_attributes(edge_index)
                    .ok()?
                    .get(key)
                    .cloned()
            })
            .collect::<GrHashSet<_>>()
            .len()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum CountKind {
    Nodes,
    Edges,
}

pub struct Count;

impl Statistic for Count {
    type Key = CountKind;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value {
        match key {
            CountKind::Nodes => graphrecord.node_indices().count(),
            CountKind::Edges => graphrecord.edge_indices().count(),
        }
    }
}
