use crate::{
    BoxedIterator,
    bool::BoolMaskOperand,
    nodes::{NodeOperand, NodeOperandContext},
    traits,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};

pub struct Filter {
    parent: BoolMaskOperand<NodeOperand>,
}

impl NodeOperandContext for Filter {
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

impl traits::Filter for NodeOperand {
    type MaskOperand = BoolMaskOperand<Self>;
    type ReturnOperand = Self;

    fn filter(&self, mask: Self::MaskOperand) -> Self::ReturnOperand {
        Self::new(Filter { parent: mask })
    }
}
