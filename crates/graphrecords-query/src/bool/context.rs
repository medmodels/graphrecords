use crate::{BoxedIterator, RootOperand};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};

pub trait BoolMaskOperandContext {
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>;
}

pub(crate) enum BoolMaskContext<O: RootOperand> {
    InGroup(Box<dyn BoolMaskOperandContext<Operand = O>>),
    Custom(Box<dyn BoolMaskOperandContext<Operand = O>>),
}

impl<O: RootOperand> BoolMaskContext<O> {
    pub(super) fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, bool)>> {
        match self {
            BoolMaskContext::InGroup(context) => context.evaluate(graphrecord),
            BoolMaskContext::Custom(context) => context.evaluate(graphrecord),
        }
    }
}
