use crate::BoxedIterator;
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::GraphRecordValue};

pub trait MultipleValuesOperandContext {
    type Index;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (&'a Self::Index, GraphRecordValue)>>;
}

pub(crate) enum MultipleValuesContext<I> {
    RootOperand(Box<dyn MultipleValuesOperandContext<Index = I>>),
    Custom(Box<dyn MultipleValuesOperandContext<Index = I>>),
}

impl<I> MultipleValuesContext<I> {
    pub(super) fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (&'a I, GraphRecordValue)>> {
        match self {
            MultipleValuesContext::RootOperand(context) => context.evaluate(graphrecord),
            MultipleValuesContext::Custom(context) => context.evaluate(graphrecord),
        }
    }
}
