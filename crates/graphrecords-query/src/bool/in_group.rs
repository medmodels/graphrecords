use crate::{
    BoxedIterator, RootOperand,
    bool::{BoolMaskContext, BoolMaskOperand, BoolMaskOperandContext},
    nodes::NodeOperand,
    traits::InGroup,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::Group};
use graphrecords_utils::aliases::GrHashSet;

pub struct NodeInGroupContext {
    parent: NodeOperand,
    group: Group,
}

impl From<NodeInGroupContext> for BoolMaskContext<NodeOperand> {
    fn from(context: NodeInGroupContext) -> Self {
        Self::InGroup(Box::new(context))
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
    type ReturnOperand = BoolMaskOperand<Self>;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        BoolMaskOperand::new(NodeInGroupContext {
            parent: self.clone(),
            group,
        })
    }
}
