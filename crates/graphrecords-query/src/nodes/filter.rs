use crate::{
    BoxedIterator, RootOperand,
    bool::BoolMaskOperand,
    nodes::{NodeOperand, NodeOperandContext},
    traits,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::NodeIndex};
use graphrecords_utils::aliases::GrHashMap;

pub struct Filter {
    operand: NodeOperand,
    mask: BoolMaskOperand<NodeOperand>,
}

impl NodeOperandContext for Filter {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a NodeIndex>> {
        let node_indices = self.operand.evaluate(graphrecord)?;

        let mask_by_index: GrHashMap<_, _> = self.mask.evaluate(graphrecord)?.collect();

        Ok(Box::new(node_indices.filter(move |node_index| {
            mask_by_index.get(node_index).copied().unwrap_or(false)
        })))
    }
}

impl traits::Filter for NodeOperand {
    type MaskOperand = BoolMaskOperand<Self>;
    type ReturnOperand = Self;

    fn filter(&self, mask: Self::MaskOperand) -> Self::ReturnOperand {
        Self::new(Filter {
            operand: self.clone(),
            mask,
        })
    }
}
