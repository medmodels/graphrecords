mod logic;

use crate::{BoxedIterator, RootOperand};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::sync::Arc;

pub trait BoolMaskOperandContext: 'static + Send + Sync {
    type Operand: RootOperand;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>;
}

pub struct BoolMaskOperand<O: RootOperand> {
    context: Arc<dyn BoolMaskOperandContext<Operand = O>>,
}

impl<O: RootOperand> Clone for BoolMaskOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> BoolMaskOperand<O> {
    pub fn new<C: BoolMaskOperandContext<Operand = O>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, bool)>> {
        self.context.evaluate(graphrecord)
    }
}
