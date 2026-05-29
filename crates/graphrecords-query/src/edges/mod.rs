mod context;
mod group_by;

use crate::{
    BoxedIterator, RootOperand,
    operand_traits::Attribute,
    values::{MultipleValuesContext, MultipleValuesOperand, MultipleValuesOperandContext},
};
pub use context::EdgeOperandContext;
pub(crate) use context::{AllEdges, EdgeContext};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct EdgeOperand {
    context: Arc<EdgeContext>,
}

impl RootOperand for EdgeOperand {
    type Index<'a> = &'a EdgeIndex;
}

impl EdgeOperand {
    pub(crate) fn new<C: Into<EdgeContext>>(context: C) -> Self {
        Self {
            context: Arc::new(context.into()),
        }
    }

    pub fn custom_context<C: EdgeOperandContext + 'static>(context: C) -> Self {
        Self {
            context: Arc::new(EdgeContext::Custom(Box::new(context))),
        }
    }

    pub(crate) fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        self.context.evaluate(graphrecord)
    }
}

pub(crate) struct EdgeAttributeContext {
    parent: EdgeOperand,
    attribute: GraphRecordAttribute,
}

impl From<EdgeAttributeContext> for MultipleValuesContext<EdgeIndex> {
    fn from(context: EdgeAttributeContext) -> Self {
        MultipleValuesContext::RootOperand(Box::new(context))
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
