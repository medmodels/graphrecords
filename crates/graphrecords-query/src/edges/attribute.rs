use crate::{
    BoxedIterator, RootOperand,
    edges::EdgeOperand,
    traits::Attribute,
    values::{MultipleValuesOperand, MultipleValuesOperandContext},
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue},
};

pub struct EdgeAttributeContext {
    parent: EdgeOperand,
    attribute: GraphRecordAttribute,
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
