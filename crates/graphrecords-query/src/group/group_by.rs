use crate::{
    BoxedIterator, RootOperand,
    edges::EdgeOperand,
    group::{
        AttributeDiscriminator, Discriminator, GroupBy, GroupOperand, GroupableOperand,
        GroupedIterator, GroupedOperandContext,
    },
    nodes::NodeOperand,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex},
};

impl GroupableOperand for NodeOperand {
    type Output<'a> = &'a NodeIndex;
}

struct NodeGroupByAttributeContext {
    parent: NodeOperand,
    discriminator: AttributeDiscriminator,
}

impl GroupedOperandContext<NodeOperand, AttributeDiscriminator> for NodeGroupByAttributeContext {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<
        GroupedIterator<
            'a,
            <AttributeDiscriminator as Discriminator>::Key<'a>,
            BoxedIterator<'a, <NodeOperand as GroupableOperand>::Output<'a>>,
        >,
    > {
        let node_indices = self.parent.evaluate(graphrecord)?;
        let attribute = &self.discriminator.attribute;

        let mut buckets: Vec<(Option<&'a GraphRecordValue>, Vec<&'a NodeIndex>)> = Vec::new();

        for node_index in node_indices {
            let value = graphrecord
                .node_attributes(node_index)
                .expect("Node must exist")
                .get(attribute);

            if let Some((_, bucket)) = buckets.iter_mut().find(|(k, _)| *k == value) {
                bucket.push(node_index);
            } else {
                buckets.push((value, vec![node_index]));
            }
        }

        Ok(Box::new(buckets.into_iter().map(|(key, group)| {
            (key, Box::new(group.into_iter()) as BoxedIterator<_>)
        })))
    }
}

impl GroupBy<AttributeDiscriminator> for NodeOperand {
    fn group_by(
        &self,
        discriminator: AttributeDiscriminator,
    ) -> GroupOperand<Self, AttributeDiscriminator> {
        GroupOperand::new(NodeGroupByAttributeContext {
            parent: self.clone(),
            discriminator,
        })
    }
}

impl GroupableOperand for EdgeOperand {
    type Output<'a> = &'a EdgeIndex;
}

struct EdgeGroupByAttributeContext {
    parent: EdgeOperand,
    discriminator: AttributeDiscriminator,
}

impl GroupedOperandContext<EdgeOperand, AttributeDiscriminator> for EdgeGroupByAttributeContext {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<
        GroupedIterator<
            'a,
            <AttributeDiscriminator as Discriminator>::Key<'a>,
            BoxedIterator<'a, <EdgeOperand as GroupableOperand>::Output<'a>>,
        >,
    > {
        let edge_indices = self.parent.evaluate(graphrecord)?;
        let attribute = &self.discriminator.attribute;

        let mut buckets: Vec<(Option<&'a GraphRecordValue>, Vec<&'a EdgeIndex>)> = Vec::new();

        for edge_index in edge_indices {
            let value = graphrecord
                .edge_attributes(edge_index)
                .expect("Edge must exist")
                .get(attribute);

            if let Some((_, bucket)) = buckets.iter_mut().find(|(k, _)| *k == value) {
                bucket.push(edge_index);
            } else {
                buckets.push((value, vec![edge_index]));
            }
        }

        Ok(Box::new(buckets.into_iter().map(|(key, group)| {
            (key, Box::new(group.into_iter()) as BoxedIterator<_>)
        })))
    }
}

impl GroupBy<AttributeDiscriminator> for EdgeOperand {
    fn group_by(
        &self,
        discriminator: AttributeDiscriminator,
    ) -> GroupOperand<Self, AttributeDiscriminator> {
        GroupOperand::new(EdgeGroupByAttributeContext {
            parent: self.clone(),
            discriminator,
        })
    }
}
