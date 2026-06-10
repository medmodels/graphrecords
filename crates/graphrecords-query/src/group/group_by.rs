use crate::{
    BoxedIterator, Explain, RootOperand,
    edges::EdgeOperand,
    execution::ExecutionContext,
    group::{
        AttributeDiscriminator, Discriminator, GroupBy, GroupOperand, GroupableOperand,
        GroupedIterator, GroupedOperandContext,
    },
    nodes::NodeOperand,
    optimizer::{HasInputs, OptimizeInputs, OptimizerHints, PlanNode},
    values::ValuesOperand,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex},
};

impl GroupableOperand for NodeOperand {
    type Grouped<'a> = BoxedIterator<'a, &'a NodeIndex>;
}

impl GroupableOperand for EdgeOperand {
    type Grouped<'a> = BoxedIterator<'a, &'a EdgeIndex>;
}

impl<O: RootOperand> GroupableOperand for ValuesOperand<O> {
    type Grouped<'a> = BoxedIterator<'a, (<O as RootOperand>::Index<'a>, GraphRecordValue)>;
}

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = GroupOperand<NodeOperand, AttributeDiscriminator>)]
#[explain(label = "GroupBy")]
struct NodeGroupByAttributeContext {
    #[input]
    input: NodeOperand,
    #[explain(label)]
    discriminator: AttributeDiscriminator,
}

impl GroupedOperandContext<NodeOperand, AttributeDiscriminator> for NodeGroupByAttributeContext {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        GroupedIterator<
            'a,
            <AttributeDiscriminator as Discriminator>::Key<'a>,
            <NodeOperand as GroupableOperand>::Grouped<'a>,
        >,
    > {
        let node_indices = self.input.evaluate(graphrecord, context)?;
        let attribute = &self.discriminator.attribute;

        let mut buckets: Vec<(Option<&'a GraphRecordValue>, Vec<&'a NodeIndex>)> = Vec::new();

        for node_index in node_indices {
            let value = graphrecord
                .node_attributes(node_index)
                .expect("Node must exist")
                .get(attribute);

            if let Some((_, bucket)) = buckets.iter_mut().find(|(key, _)| *key == value) {
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
            input: self.clone(),
            discriminator,
        })
    }
}

#[derive(PlanNode, HasInputs, OptimizeInputs, OptimizerHints, Explain)]
#[plan(operand = GroupOperand<EdgeOperand, AttributeDiscriminator>)]
#[explain(label = "GroupBy")]
struct EdgeGroupByAttributeContext {
    #[input]
    input: EdgeOperand,
    #[explain(label)]
    discriminator: AttributeDiscriminator,
}

impl GroupedOperandContext<EdgeOperand, AttributeDiscriminator> for EdgeGroupByAttributeContext {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<
        GroupedIterator<
            'a,
            <AttributeDiscriminator as Discriminator>::Key<'a>,
            <EdgeOperand as GroupableOperand>::Grouped<'a>,
        >,
    > {
        let edge_indices = self.input.evaluate(graphrecord, context)?;
        let attribute = &self.discriminator.attribute;

        let mut buckets: Vec<(Option<&'a GraphRecordValue>, Vec<&'a EdgeIndex>)> = Vec::new();

        for edge_index in edge_indices {
            let value = graphrecord
                .edge_attributes(edge_index)
                .expect("Edge must exist")
                .get(attribute);

            if let Some((_, bucket)) = buckets.iter_mut().find(|(key, _)| *key == value) {
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
            input: self.clone(),
            discriminator,
        })
    }
}
