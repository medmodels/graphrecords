mod discriminator;
mod group_by;

use crate::BoxedIterator;
pub use discriminator::{AttributeDiscriminator, Discriminator};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::sync::Arc;

pub type GroupedIterator<'a, K, T> = BoxedIterator<'a, (K, T)>;

pub trait GroupableOperand {
    type Output<'a>;
}

pub trait GroupedOperandContext<O: GroupableOperand, D: Discriminator> {
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<GroupedIterator<'a, D::Key<'a>, BoxedIterator<'a, O::Output<'a>>>>;
}

#[derive(Clone)]
pub struct GroupOperand<O, D: Discriminator> {
    context: Arc<dyn GroupedOperandContext<O, D>>,
}

impl<O: GroupableOperand, D: Discriminator> GroupOperand<O, D> {
    pub fn new<C: GroupedOperandContext<O, D> + 'static>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }

    pub fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<GroupedIterator<'a, D::Key<'a>, BoxedIterator<'a, O::Output<'a>>>> {
        self.context.evaluate(graphrecord)
    }
}

pub trait GroupBy<D: Discriminator>: Sized {
    fn group_by(&self, discriminator: D) -> GroupOperand<Self, D>;
}
