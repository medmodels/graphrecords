use crate::{
    BoxedIterator, RootOperand,
    nodes::NodeOperand,
    traits::Attribute,
    values::{MultipleValuesOperand, MultipleValuesOperandContext},
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{GraphRecordAttribute, GraphRecordValue, NodeIndex},
};

pub struct NodeAttributeContext {
    parent: NodeOperand,
    attribute: GraphRecordAttribute,
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
