use graphrecords_core::{
    GraphRecord,
    graphrecord::{AttributeName, Group, StateView},
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

        #[expect(clippy::missing_panics_doc, reason = "infallible")]
        let map = cache
            .entry(TypeId::of::<S>())
            .or_insert_with(|| Box::new(GrHashMap::<S::Key, S::Value>::default()))
            .downcast_mut::<GrHashMap<S::Key, S::Value>>()
            .expect("Statistic cache type must match its TypeId key");

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
        let state = StateView::of(graphrecord);

        state
            .resolve_group_address(key)
            .map_or(0, |group_address| state.group_node_count(group_address))
    }
}

pub struct EdgeGroupSize;

impl Statistic for EdgeGroupSize {
    type Key = Group;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value {
        let state = StateView::of(graphrecord);

        state
            .resolve_group_address(key)
            .map_or(0, |group_address| state.group_edge_count(group_address))
    }
}

pub struct NodeAttributeCardinality;

impl Statistic for NodeAttributeCardinality {
    type Key = AttributeName;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value {
        let state = StateView::of(graphrecord);

        state
            .resolve_node_attribute_address(key)
            .map_or(0, |attribute_address| {
                state
                    .node_addresses()
                    .filter_map(|node_address| {
                        state.node_attribute(node_address, attribute_address)
                    })
                    .collect::<GrHashSet<_>>()
                    .len()
            })
    }
}

pub struct EdgeAttributeCardinality;

impl Statistic for EdgeAttributeCardinality {
    type Key = AttributeName;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value {
        let state = StateView::of(graphrecord);

        state
            .resolve_edge_attribute_address(key)
            .map_or(0, |attribute_address| {
                state
                    .edge_addresses()
                    .filter_map(|edge_address| {
                        state.edge_attribute(edge_address, attribute_address)
                    })
                    .collect::<GrHashSet<_>>()
                    .len()
            })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum CountKind {
    Nodes,
    Edges,
    Groups,
}

pub struct Count;

impl Statistic for Count {
    type Key = CountKind;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, key: &Self::Key) -> Self::Value {
        match key {
            CountKind::Nodes => graphrecord.node_indices().count(),
            CountKind::Edges => graphrecord.edge_indices().count(),
            CountKind::Groups => graphrecord.group_count(),
        }
    }
}
