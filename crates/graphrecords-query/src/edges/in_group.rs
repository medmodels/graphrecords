use crate::{
    BoxedIterator, EdgeOperand, RootOperand,
    bool::{BoolMaskOperand, BoolMaskOperandContext},
    traits::InGroup,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::Group};
use graphrecords_utils::aliases::GrHashSet;

struct InGroupContext {
    parent: EdgeOperand,
    group: Group,
}

impl BoolMaskOperandContext for InGroupContext {
    type Operand = EdgeOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        let edge_indices = self.parent.evaluate(graphrecord)?;

        let edge_indices_in_group: GrHashSet<_> =
            graphrecord.edges_in_group(&self.group)?.collect();

        Ok(Box::new(edge_indices.map(move |edge_index| {
            let in_group = edge_indices_in_group.contains(&edge_index);

            (edge_index, in_group)
        })))
    }
}

impl InGroup for EdgeOperand {
    type ReturnOperand = BoolMaskOperand<Self>;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        BoolMaskOperand::new(InGroupContext {
            parent: self.clone(),
            group,
        })
    }
}
