use crate::{
    BoxedIterator, EdgeOperand, RootOperand, bool::BoolMaskOperand, edges::EdgeOperandContext,
    traits,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::EdgeIndex};
use graphrecords_utils::aliases::GrHashMap;

pub struct Filter {
    operand: EdgeOperand,
    mask: BoolMaskOperand<EdgeOperand>,
}

impl EdgeOperandContext for Filter {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a EdgeIndex>> {
        let edge_indices = self.operand.evaluate(graphrecord)?;

        let mask_by_index: GrHashMap<_, _> = self.mask.evaluate(graphrecord)?.collect();

        Ok(Box::new(edge_indices.filter(move |edge_index| {
            mask_by_index.get(edge_index).copied().unwrap_or(false)
        })))
    }
}

impl traits::Filter for EdgeOperand {
    type MaskOperand = BoolMaskOperand<Self>;
    type ReturnOperand = Self;

    fn filter(&self, mask: Self::MaskOperand) -> Self::ReturnOperand {
        Self::new(Filter {
            operand: self.clone(),
            mask,
        })
    }
}
