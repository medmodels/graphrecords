mod in_group;

use crate::{BoxedIterator, RootOperand};
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

pub trait BoolMaskOperandContext: Send + Sync {
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
    pub(crate) fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, bool)>> {
        match self {
            Self::InGroup(context) | Self::Custom(context) => context.evaluate(graphrecord),
        }
    }
}
