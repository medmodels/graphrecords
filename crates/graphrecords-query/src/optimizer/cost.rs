use crate::{Operand, operations::Operation};
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

    fn compute(graphrecord: &GraphRecord, group: &Self::Key) -> Self::Value {
        graphrecord.nodes_in_group(group).map_or(0, Iterator::count)
    }
}

pub struct EdgeGroupSize;

impl Statistic for EdgeGroupSize {
    type Key = Group;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, group: &Self::Key) -> Self::Value {
        graphrecord.edges_in_group(group).map_or(0, Iterator::count)
    }
}

pub struct NodeAttributeCardinality;

impl Statistic for NodeAttributeCardinality {
    type Key = GraphRecordAttribute;
    type Value = usize;

    fn compute(graphrecord: &GraphRecord, attribute: &Self::Key) -> Self::Value {
        graphrecord
            .node_indices()
            .filter_map(|node_index| {
                graphrecord
                    .node_attributes(node_index)
                    .ok()?
                    .get(attribute)
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

    fn compute(graphrecord: &GraphRecord, attribute: &Self::Key) -> Self::Value {
        graphrecord
            .edge_indices()
            .filter_map(|edge_index| {
                graphrecord
                    .edge_attributes(edge_index)
                    .ok()?
                    .get(attribute)
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

pub trait Cost<O: Operand> {
    fn cost(&self, stats: &Stats) -> O::Cost;
}

pub trait EstimateCost<P: Operation>: Operand {
    type OutputCost;

    fn estimate(
        operation: &P,
        input_cost: <Self as Operand>::Cost,
        stats: &Stats,
    ) -> Self::OutputCost;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cardinality(pub usize);

impl Cardinality {
    #[must_use]
    pub fn scaled(self, selectivity: Selectivity) -> Self {
        Self((self.0 as f64 * selectivity.0).round() as usize)
    }

    #[must_use]
    pub fn split(self, groups: Self) -> (Self, Self) {
        let groups = Self(groups.0.min(self.0));
        let per_group = Self(if groups.0 == 0 {
            0
        } else {
            self.0.div_ceil(groups.0)
        });

        (groups, per_group)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupCost<Inner> {
    pub groups: Cardinality,
    pub per_group: Inner,
}

impl<Inner> GroupCost<Inner> {
    #[must_use]
    pub fn map<T>(self, transform: impl FnOnce(Inner) -> T) -> GroupCost<T> {
        GroupCost {
            groups: self.groups,
            per_group: transform(self.per_group),
        }
    }
}

impl GroupCost<Cardinality> {
    #[must_use]
    pub const fn total(self) -> Cardinality {
        Cardinality(self.groups.0 * self.per_group.0)
    }
}

impl GroupCost<ValueCost> {
    #[must_use]
    pub fn total(self) -> ValueCost {
        ValueCost::new(
            Cardinality(self.groups.0 * self.per_group.rows.0),
            Cardinality(self.groups.0 * self.per_group.distinct.0),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueCost {
    rows: Cardinality,
    distinct: Cardinality,
}

impl ValueCost {
    #[must_use]
    pub fn new(rows: Cardinality, distinct: Cardinality) -> Self {
        Self {
            rows,
            distinct: Cardinality(distinct.0.min(rows.0)),
        }
    }

    #[must_use]
    pub const fn unknown(rows: Cardinality) -> Self {
        Self {
            rows,
            distinct: rows,
        }
    }

    #[must_use]
    pub const fn rows(&self) -> Cardinality {
        self.rows
    }

    #[must_use]
    pub const fn distinct(&self) -> Cardinality {
        self.distinct
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Selectivity(f64);

impl Selectivity {
    #[must_use]
    pub const fn new(value: f64) -> Self {
        Self(value.clamp(0.0, 1.0))
    }

    #[must_use]
    pub fn ratio(part: Cardinality, whole: Cardinality) -> Self {
        Self::new(part.0 as f64 / whole.0.max(1) as f64)
    }

    #[must_use]
    pub fn negated(self) -> Self {
        Self::new(1.0 - self.0)
    }

    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self::new(self.0 * other.0)
    }

    #[must_use]
    pub fn or(self, other: Self) -> Self {
        Self::new(self.0.mul_add(-other.0, self.0 + other.0))
    }

    #[must_use]
    pub fn xor(self, other: Self) -> Self {
        Self::new((2.0 * self.0).mul_add(-other.0, self.0 + other.0))
    }
}
