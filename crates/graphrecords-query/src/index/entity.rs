use crate::{
    index::EntityIndexDomain,
    optimizer::{
        EdgeAttributeCardinality, EdgeGroupSize, NodeAttributeCardinality, NodeGroupSize, Stats,
    },
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{
        AttributeName, AttributeNameView, EdgeAttributeAddress, EdgeIndex, GroupAddress,
        GroupIndex, NodeAttributeAddress, NodeIndex, StateView, ValueView,
    },
};

pub trait EntityAttributes: EntityIndexDomain {
    type AttributeAddress: Copy;

    fn attribute_addresses(
        graphrecord: &GraphRecord,
    ) -> impl Iterator<Item = Self::AttributeAddress> + '_;

    fn attribute<'a>(
        graphrecord: &'a GraphRecord,
        address: &Self::Address,
        attribute_address: Self::AttributeAddress,
    ) -> Option<ValueView<'a>>;

    fn attribute_name(
        graphrecord: &GraphRecord,
        attribute_address: Self::AttributeAddress,
    ) -> AttributeNameView<'_>;

    fn resolve_attribute_address(
        graphrecord: &GraphRecord,
        attribute_name: &AttributeName,
    ) -> Option<Self::AttributeAddress>;

    fn attribute_cardinality(stats: &Stats, attribute: &AttributeName) -> usize;
}

impl EntityAttributes for NodeIndex {
    type AttributeAddress = NodeAttributeAddress;

    fn attribute_addresses(
        graphrecord: &GraphRecord,
    ) -> impl Iterator<Item = Self::AttributeAddress> + '_ {
        StateView::of(graphrecord).node_attribute_addresses()
    }

    fn attribute<'a>(
        graphrecord: &'a GraphRecord,
        address: &Self::Address,
        attribute_address: Self::AttributeAddress,
    ) -> Option<ValueView<'a>> {
        StateView::of(graphrecord).node_attribute(*address, attribute_address)
    }

    fn attribute_name(
        graphrecord: &GraphRecord,
        attribute_address: Self::AttributeAddress,
    ) -> AttributeNameView<'_> {
        AttributeNameView::from(
            StateView::of(graphrecord)
                .node_attribute_name(attribute_address)
                .identifier(),
        )
    }

    fn resolve_attribute_address(
        graphrecord: &GraphRecord,
        attribute_name: &AttributeName,
    ) -> Option<Self::AttributeAddress> {
        StateView::of(graphrecord).resolve_node_attribute_address(attribute_name)
    }

    fn attribute_cardinality(stats: &Stats, attribute: &AttributeName) -> usize {
        stats.get::<NodeAttributeCardinality>(attribute)
    }
}

impl EntityAttributes for EdgeIndex {
    type AttributeAddress = EdgeAttributeAddress;

    fn attribute_addresses(
        graphrecord: &GraphRecord,
    ) -> impl Iterator<Item = Self::AttributeAddress> + '_ {
        StateView::of(graphrecord).edge_attribute_addresses()
    }

    fn attribute<'a>(
        graphrecord: &'a GraphRecord,
        address: &Self::Address,
        attribute_address: Self::AttributeAddress,
    ) -> Option<ValueView<'a>> {
        StateView::of(graphrecord).edge_attribute(*address, attribute_address)
    }

    fn attribute_name(
        graphrecord: &GraphRecord,
        attribute_address: Self::AttributeAddress,
    ) -> AttributeNameView<'_> {
        AttributeNameView::from(
            StateView::of(graphrecord)
                .edge_attribute_name(attribute_address)
                .identifier(),
        )
    }

    fn resolve_attribute_address(
        graphrecord: &GraphRecord,
        attribute_name: &AttributeName,
    ) -> Option<Self::AttributeAddress> {
        StateView::of(graphrecord).resolve_edge_attribute_address(attribute_name)
    }

    fn attribute_cardinality(stats: &Stats, attribute: &AttributeName) -> usize {
        stats.get::<EdgeAttributeCardinality>(attribute)
    }
}

pub trait GroupMembership: EntityIndexDomain {
    fn addresses_in_group(
        graphrecord: &GraphRecord,
        group_address: GroupAddress,
    ) -> impl Iterator<Item = Self::Address> + '_;

    fn group_addresses<'a>(
        graphrecord: &'a GraphRecord,
        address: &Self::Address,
    ) -> impl Iterator<Item = GroupAddress> + 'a;

    fn group_size(stats: &Stats, group_index: &GroupIndex) -> usize;
}

impl GroupMembership for NodeIndex {
    fn addresses_in_group(
        graphrecord: &GraphRecord,
        group_address: GroupAddress,
    ) -> impl Iterator<Item = Self::Address> + '_ {
        StateView::of(graphrecord).group_node_addresses(group_address)
    }

    fn group_addresses<'a>(
        graphrecord: &'a GraphRecord,
        address: &Self::Address,
    ) -> impl Iterator<Item = GroupAddress> + 'a {
        StateView::of(graphrecord).node_group_addresses(*address)
    }

    fn group_size(stats: &Stats, group_index: &GroupIndex) -> usize {
        stats.get::<NodeGroupSize>(group_index)
    }
}

impl GroupMembership for EdgeIndex {
    fn addresses_in_group(
        graphrecord: &GraphRecord,
        group_address: GroupAddress,
    ) -> impl Iterator<Item = Self::Address> + '_ {
        StateView::of(graphrecord).group_edge_addresses(group_address)
    }

    fn group_addresses<'a>(
        graphrecord: &'a GraphRecord,
        address: &Self::Address,
    ) -> impl Iterator<Item = GroupAddress> + 'a {
        StateView::of(graphrecord).edge_group_addresses(*address)
    }

    fn group_size(stats: &Stats, group_index: &GroupIndex) -> usize {
        stats.get::<EdgeGroupSize>(group_index)
    }
}
