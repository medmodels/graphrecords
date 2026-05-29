mod context;

use crate::{BoxedIterator, RootOperand};
pub(crate) use context::BoolMaskContext;
pub use context::BoolMaskOperandContext;
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::sync::Arc;

#[derive(Clone)]
pub struct BoolMaskOperand<O: RootOperand> {
    context: Arc<BoolMaskContext<O>>,
}

impl<O: RootOperand> BoolMaskOperand<O> {
    pub(crate) fn new<C: Into<BoolMaskContext<O>>>(context: C) -> Self {
        Self {
            context: Arc::new(context.into()),
        }
    }

    pub fn custom_context<C: BoolMaskOperandContext<Operand = O> + 'static>(context: C) -> Self {
        Self {
            context: Arc::new(BoolMaskContext::Custom(Box::new(context))),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, bool)>> {
        self.context.evaluate(graphrecord)
    }
}
