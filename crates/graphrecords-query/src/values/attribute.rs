use crate::{
    BoxedIterator,
    edges::EdgeOperand,
    nodes::NodeOperand,
    traits::Attribute,
    values::{MultipleValuesContext, MultipleValuesOperand, MultipleValuesOperandContext},
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex},
};

pub struct NodeAttributeContext {
    parent: NodeOperand,
    attribute: GraphRecordAttribute,
}

impl From<NodeAttributeContext> for MultipleValuesContext<NodeIndex> {
    fn from(context: NodeAttributeContext) -> Self {
        Self::RootOperand(Box::new(context))
    }
}

impl MultipleValuesOperandContext for NodeAttributeContext {
    type Index = NodeIndex;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (&'a Self::Index, GraphRecordValue)>> {
        let node_indices = self.parent.evaluate(graphrecord)?;

        Ok(Box::new(node_indices.filter_map(|node_index| {
            let value = graphrecord
                .node_attributes(node_index)
                .expect("Node must exist")
                .get(&self.attribute)?
                .clone();

            Some((node_index, value))
        })))
    }
}

impl Attribute for NodeOperand {
    type ReturnOperand = MultipleValuesOperand<NodeIndex>;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        MultipleValuesOperand::new(NodeAttributeContext {
            parent: self.clone(),
            attribute,
        })
    }
}

pub struct EdgeAttributeContext {
    parent: EdgeOperand,
    attribute: GraphRecordAttribute,
}

impl From<EdgeAttributeContext> for MultipleValuesContext<EdgeIndex> {
    fn from(context: EdgeAttributeContext) -> Self {
        Self::RootOperand(Box::new(context))
    }
}

impl MultipleValuesOperandContext for EdgeAttributeContext {
    type Index = EdgeIndex;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (&'a Self::Index, GraphRecordValue)>> {
        let edge_indices = self.parent.evaluate(graphrecord)?;

        Ok(Box::new(edge_indices.filter_map(|edge_index| {
            let value = graphrecord
                .edge_attributes(edge_index)
                .expect("Edge must exist")
                .get(&self.attribute)?
                .clone();

            Some((edge_index, value))
        })))
    }
}

impl Attribute for EdgeOperand {
    type ReturnOperand = MultipleValuesOperand<EdgeIndex>;

    fn attribute(&self, attribute: GraphRecordAttribute) -> Self::ReturnOperand {
        MultipleValuesOperand::new(EdgeAttributeContext {
            parent: self.clone(),
            attribute,
        })
    }
}
