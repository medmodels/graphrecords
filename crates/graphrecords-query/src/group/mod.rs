mod discriminator;

use crate::{
    BoxedIterator, EvaluateContext, EvaluateOperand, Explain, Operand,
    execution::ExecutionContext,
    optimizer::{Cardinality, OptimizeInputs, PlanNode},
};
pub use discriminator::{AttributeDiscriminator, Discriminator};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult};
use std::sync::Arc;

pub type GroupedIterator<'a, K, T> = BoxedIterator<'a, (K, T)>;

pub fn partition_by<'a, Key, Item>(
    items: BoxedIterator<'a, Item>,
    key_of: impl Fn(&Item) -> Key,
) -> GroupedIterator<'a, Key, BoxedIterator<'a, Item>>
where
    Key: PartialEq + 'a,
    Item: 'a,
{
    let mut buckets: Vec<(Key, Vec<Item>)> = Vec::new();

    for item in items {
        let key = key_of(&item);

        if let Some((_, bucket)) = buckets.iter_mut().find(|(existing, _)| *existing == key) {
            bucket.push(item);
        } else {
            buckets.push((key, vec![item]));
        }
    }

    Box::new(
        buckets
            .into_iter()
            .map(|(key, items)| (key, Box::new(items.into_iter()) as BoxedIterator<'a, Item>)),
    )
}

pub fn map_partitions<'a, Key, In, Out>(
    partitions: GroupedIterator<'a, Key, In>,
    transform: impl Fn(In) -> Out + 'a,
) -> GroupedIterator<'a, Key, Out>
where
    Key: 'a,
    In: 'a,
    Out: 'a,
{
    Box::new(partitions.map(move |(key, partition)| (key, transform(partition))))
}

pub trait GroupedOperandContext<O: Operand, D: Discriminator>:
    PlanNode
    + OptimizeInputs<Output = GroupOperand<O, D>>
    + Cardinality
    + Explain
    + EvaluateContext<Operand = GroupOperand<O, D>>
{
}

pub struct GroupOperand<O: Operand + ?Sized, D: Discriminator> {
    context: Arc<dyn GroupedOperandContext<O, D>>,
}

impl<O: Operand, D: Discriminator> Clone for GroupOperand<O, D> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<O: Operand, D: Discriminator> Operand for GroupOperand<O, D> {
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

impl<O: Operand, D: Discriminator> EvaluateOperand for GroupOperand<O, D> {
    type ReturnValue<'a> = GroupedIterator<'a, D::Key<'a>, O::ReturnValue<'a>>;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        context: &'a ExecutionContext<'a>,
    ) -> GraphRecordResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, context)
    }
}

impl<O: Operand, D: Discriminator> GroupOperand<O, D> {
    #[must_use]
    pub fn new<C: GroupedOperandContext<O, D>>(context: C) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

pub trait GroupBy<D: Discriminator>: Operand {
    type Output: Operand;

    fn group_by(&self, discriminator: D) -> Self::Output;
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
