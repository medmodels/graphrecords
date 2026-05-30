use crate::{
    BoxedIterator, RootOperand,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
    nodes::NodeOperand,
    traits::InGroup,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::Group};
use graphrecords_utils::aliases::GrHashSet;

struct InGroupContext {
    parent: NodeOperand,
    group: Group,
}

impl BoolMaskOperandContext for InGroupContext {
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
        BoolMaskOperand::new(InGroupContext {
            parent: self.clone(),
            group,
        })
    }
}
