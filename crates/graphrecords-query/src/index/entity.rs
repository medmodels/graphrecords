use crate::{
    Failure, QueryResult,
    index::EntityDomain,
    optimizer::{
        EdgeAttributeCardinality, EdgeGroupSize, NodeAttributeCardinality, NodeGroupSize, Stats,
    },
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordError,
    graphrecord::{AttributeMap, AttributeName, EdgeIndex, Group, NodeIndex},
};
use graphrecords_utils::aliases::GrHashSet;

pub trait EntityAttributes: EntityDomain {
    fn attributes<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Index<'a>,
    ) -> Result<&'a AttributeMap, GraphRecordError>;

    fn attribute_cardinality(stats: &Stats, attribute: &AttributeName) -> usize;
}

impl EntityAttributes for NodeIndex {
    fn attributes<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Index<'a>,
    ) -> Result<&'a AttributeMap, GraphRecordError> {
        graphrecord.node_attributes(index)
    }

    fn attribute_cardinality(stats: &Stats, attribute: &AttributeName) -> usize {
        stats.get::<NodeAttributeCardinality>(attribute)
    }
}

impl EntityAttributes for EdgeIndex {
    fn attributes<'a>(
        graphrecord: &'a GraphRecord,
        index: &Self::Index<'a>,
    ) -> Result<&'a AttributeMap, GraphRecordError> {
        graphrecord.edge_attributes(index)
    }

    fn attribute_cardinality(stats: &Stats, attribute: &AttributeName) -> usize {
        stats.get::<EdgeAttributeCardinality>(attribute)
    }
}

pub trait IndicesInGroup: EntityDomain {
    fn indices_in_group<'a>(
        label: &'static str,
        graphrecord: &'a GraphRecord,
        group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>>;

    fn group_size(stats: &Stats, group: &Group) -> usize;
}

impl IndicesInGroup for NodeIndex {
    fn indices_in_group<'a>(
        label: &'static str,
        graphrecord: &'a GraphRecord,
        group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>> {
        Ok(graphrecord
            .nodes_in_group(group)
            .map_err(|error| Failure::new(label, error))?
            .collect())
    }

    fn group_size(stats: &Stats, group: &Group) -> usize {
        stats.get::<NodeGroupSize>(group)
    }
}

impl IndicesInGroup for EdgeIndex {
    fn indices_in_group<'a>(
        label: &'static str,
        graphrecord: &'a GraphRecord,
        group: &Group,
    ) -> QueryResult<GrHashSet<Self::Index<'a>>> {
        Ok(graphrecord
            .edges_in_group(group)
            .map_err(|error| Failure::new(label, error))?
            .collect())
    }

    fn group_size(stats: &Stats, group: &Group) -> usize {
        stats.get::<EdgeGroupSize>(group)
    }
}
