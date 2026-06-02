mod discriminator;
mod group_by;

use crate::{BoxedIterator, execution::ExecutionContext};
pub use discriminator::{AttributeDiscriminator, Discriminator};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::sync::Arc;

pub type GroupedIterator<'a, K, T> = BoxedIterator<'a, (K, T)>;

pub trait GroupableOperand: 'static {
    type Output<'a>;
}

pub trait GroupedOperandContext<O: GroupableOperand, D: Discriminator>: 'static {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<GroupedIterator<'a, D::Key<'a>, BoxedIterator<'a, O::Output<'a>>>>;
}

pub struct GroupOperand<O, D: Discriminator> {
    context: Arc<dyn GroupedOperandContext<O, D>>,
}

impl<O, D: Discriminator> Clone for GroupOperand<O, D> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: GroupableOperand, D: Discriminator> GroupOperand<O, D> {
    #[must_use]
    pub fn new<C: GroupedOperandContext<O, D>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<GroupedIterator<'a, D::Key<'a>, BoxedIterator<'a, O::Output<'a>>>> {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait GroupBy<D: Discriminator>: Sized {
    fn group_by(&self, discriminator: D) -> GroupOperand<Self, D>;
}
