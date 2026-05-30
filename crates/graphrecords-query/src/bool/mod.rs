mod in_group;
mod logic;

use crate::{
    BoxedIterator, RootOperand,
    bool::{
        in_group::InGroupContext,
        logic::{AndContext, NotContext, OrContext},
    },
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::sync::Arc;

pub struct BoolMaskOperand<O: RootOperand> {
    context: Arc<BoolMaskContext<O>>,
}

impl<O: RootOperand> Clone for BoolMaskOperand<O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: RootOperand> BoolMaskOperand<O> {
    fn new<C: Into<BoolMaskContext<O>>>(context: C) -> Self {
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

enum BoolMaskContext<O: RootOperand> {
    And(AndContext<O>),
    Or(OrContext<O>),
    Not(NotContext<O>),
    InGroup(InGroupContext<O>),
    Custom(Box<dyn BoolMaskOperandContext<Operand = O>>),
}

impl<O: RootOperand> BoolMaskContext<O> {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, bool)>> {
        match self {
            Self::And(context) => context.evaluate(graphrecord),
            Self::Or(context) => context.evaluate(graphrecord),
            Self::Not(context) => context.evaluate(graphrecord),
            Self::InGroup(context) => context.evaluate(graphrecord),
            Self::Custom(context) => context.evaluate(graphrecord),
        }
    }
}
