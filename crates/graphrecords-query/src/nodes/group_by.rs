use crate::{
    BoxedIterator, NodeOperand,
    group_by::{
        AttributeDiscriminator, Discriminator, GroupBy, GroupOperand, GroupableOperand,
        GroupedIterator, GroupedOperandContext,
    },
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{GraphRecordValue, NodeIndex},
};

impl GroupableOperand for NodeOperand {
    type Output<'a> = &'a NodeIndex;
}

struct GroupByAttributeContext {
    parent: NodeOperand,
    discriminator: AttributeDiscriminator,
}

impl GroupedOperandContext<NodeOperand, AttributeDiscriminator> for GroupByAttributeContext {
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
        GroupOperand::new(GroupByAttributeContext {
            parent: self.clone(),
            discriminator,
        })
    }
}
