mod context;
mod group_by;

use crate::{
    BoxedIterator, InGroup, RootOperand,
    bool::{BoolMaskContext, BoolMaskOperand, BoolMaskOperandContext},
    operand_traits::{self, Attribute},
    values::{MultipleValuesContext, MultipleValuesOperand, MultipleValuesOperandContext},
};
pub use context::NodeOperandContext;
pub(crate) use context::{AllNodes, NodeContext};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{GraphRecordAttribute, GraphRecordValue, Group, NodeIndex},
};
use graphrecords_utils::aliases::GrHashSet;
use std::sync::Arc;

#[derive(Clone)]
pub struct NodeOperand {
    context: Arc<NodeContext>,
}

impl RootOperand for NodeOperand {
    type Index<'a> = &'a NodeIndex;
}

impl NodeOperand {
    pub(crate) fn new<C: Into<NodeContext>>(context: C) -> Self {
        Self {
            context: Arc::new(context.into()),
        }
    }

    pub fn custom_context<C: NodeOperandContext + 'static>(context: C) -> Self {
        Self {
            context: Arc::new(NodeContext::Custom(Box::new(context))),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>> {
        self.context.evaluate(graphrecord)
    }
}

pub(crate) struct NodeInGroupContext {
    parent: NodeOperand,
    group: Group,
}

impl From<NodeInGroupContext> for BoolMaskContext<NodeOperand> {
    fn from(context: NodeInGroupContext) -> Self {
        BoolMaskContext::InGroup(Box::new(context))
    }
}

impl BoolMaskOperandContext for NodeInGroupContext {
    type Operand = NodeOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let node_indices = self.parent.evaluate(graphrecord)?;

        let node_indices_in_group: GrHashSet<_> =
            graphrecord.nodes_in_group(&self.group)?.collect();

        Ok(Box::new(node_indices.map(move |node_index| {
            let in_group = node_indices_in_group.contains(&node_index);

            (node_index, in_group)
        })))
    }
}

impl InGroup for NodeOperand {
    type ReturnOperand = BoolMaskOperand<NodeOperand>;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        BoolMaskOperand::new(NodeInGroupContext {
            parent: self.clone(),
            group,
        })
    }
}

pub(crate) struct NodeAttributeContext {
    parent: NodeOperand,
    attribute: GraphRecordAttribute,
}

impl From<NodeAttributeContext> for MultipleValuesContext<NodeIndex> {
    fn from(context: NodeAttributeContext) -> Self {
        MultipleValuesContext::RootOperand(Box::new(context))
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

pub(crate) struct Where {
    parent: BoolMaskOperand<NodeOperand>,
}

impl From<Where> for NodeContext {
    fn from(context: Where) -> Self {
        NodeContext::Where(context)
    }
}

impl NodeOperandContext for Where {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>> {
        let node_indices_with_mask = self.parent.evaluate(graphrecord)?;

        Ok(Box::new(node_indices_with_mask.filter_map(
            |(node_index, mask)| {
                if mask { Some(node_index) } else { None }
            },
        )))
    }
}

impl operand_traits::Where for NodeOperand {
    type MaskOperand = BoolMaskOperand<NodeOperand>;
    type ReturnOperand = NodeOperand;

    fn r#where(&self, predicate: Self::MaskOperand) -> Self::ReturnOperand {
        NodeOperand::new(Where { parent: predicate })
    }
}
