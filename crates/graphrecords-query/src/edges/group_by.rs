use crate::{
    BoxedIterator,
    edges::EdgeOperand,
    group_by::{
        AttributeDiscriminator, Discriminator, GroupBy, GroupOperand, GroupableOperand,
        GroupedIterator, GroupedOperandContext,
    },
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordValue},
};

impl GroupableOperand for EdgeOperand {
    type Output<'a> = &'a EdgeIndex;
}

struct GroupByAttributeContext {
    parent: EdgeOperand,
    discriminator: AttributeDiscriminator,
}

impl GroupedOperandContext<EdgeOperand, AttributeDiscriminator> for GroupByAttributeContext {
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
        GroupOperand::new(GroupByAttributeContext {
            parent: self.clone(),
            discriminator,
        })
    }
}
