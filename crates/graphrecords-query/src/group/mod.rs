mod discriminator;
mod group_by;

use crate::{
    BoxedIterator, Explain, Operand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
pub use discriminator::{AttributeDiscriminator, Discriminator};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::sync::Arc;

pub type GroupedIterator<'a, K, T> = BoxedIterator<'a, (K, T)>;

pub trait GroupableOperand: Operand + Clone + 'static {
    type Grouped<'a>;
}

pub trait GroupedOperandContext<O: GroupableOperand, D: Discriminator>:
    PlanNode + OptimizeInputs<Output = GroupOperand<O, D>> + Cardinality + Explain
{
    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<GroupedIterator<'a, D::Key<'a>, O::Grouped<'a>>>;
}

pub struct GroupOperand<O: GroupableOperand, D: Discriminator> {
    context: Arc<dyn GroupedOperandContext<O, D>>,
}

impl<O: GroupableOperand, D: Discriminator> Clone for GroupOperand<O, D> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: GroupableOperand, D: Discriminator> Operand for GroupOperand<O, D> {
    type Context = dyn GroupedOperandContext<O, D>;

    fn context(&self) -> &Self::Context {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<Self::Context>) -> Self {
        Self { context }
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
    ) -> GraphRecordResult<GroupedIterator<'a, D::Key<'a>, O::Grouped<'a>>> {
        self.context.evaluate(graphrecord, context)
    }
}

pub trait GroupBy<D: Discriminator>: GroupableOperand {
    fn group_by(&self, discriminator: D) -> GroupOperand<Self, D>;
}

#[cfg(test)]
mod test {
    use crate::{
        QueryNodes,
        group::{AttributeDiscriminator, GroupBy},
        traits::Attribute,
    };
    use graphrecords_core::{GraphRecord, graphrecord::GraphRecordValue};
    use std::collections::HashMap;

    #[test]
    fn test_group_by_attribute_then_attribute() {
        let graphrecord = GraphRecord::from_tuples(
            vec![
                (
                    "0".into(),
                    HashMap::from([("color".into(), "red".into()), ("age".into(), 10.into())]),
                ),
                (
                    "1".into(),
                    HashMap::from([("color".into(), "red".into()), ("age".into(), 20.into())]),
                ),
                (
                    "2".into(),
                    HashMap::from([("color".into(), "blue".into()), ("age".into(), 30.into())]),
                ),
            ],
            None,
            None,
        )
        .unwrap();

        let selection = QueryNodes::query_nodes(&graphrecord, |node| {
            node.group_by(AttributeDiscriminator {
                attribute: "color".into(),
            })
            .attribute("age".into())
        });

        let mut groups: Vec<(Option<GraphRecordValue>, Vec<GraphRecordValue>)> = selection
            .evaluate()
            .unwrap()
            .map(|(key, values)| {
                let mut values = values.map(|(_, value)| value).collect::<Vec<_>>();
                values.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));

                (key.cloned(), values)
            })
            .collect();

        groups.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));

        assert_eq!(
            vec![
                (Some("blue".into()), vec![30.into()]),
                (Some("red".into()), vec![10.into(), 20.into()]),
            ],
            groups
        );
    }
}
