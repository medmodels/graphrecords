pub mod attributes;
pub mod edges;
pub mod group_by;
pub mod nodes;
mod operand_traits;
pub mod values;
pub mod wrapper;

use super::{EdgeIndex, GraphRecord, GraphRecordAttribute, GraphRecordValue, NodeIndex, Wrapper};
use crate::{
    errors::GraphRecordResult,
    graphrecord::querying::{
        attributes::{
            EdgeMultipleAttributesWithoutIndexOperand, EdgeSingleAttributeWithoutIndexOperand,
            NodeMultipleAttributesWithoutIndexOperand, NodeSingleAttributeWithoutIndexOperand,
        },
        group_by::{GroupBy, GroupKey, PartitionGroups},
        values::{EdgeSingleValueWithoutIndexOperand, NodeSingleValueWithoutIndexOperand},
    },
};
use attributes::{
    EdgeAttributesTreeOperand, EdgeMultipleAttributesWithIndexOperand,
    EdgeSingleAttributeWithIndexOperand, GetAllAttributes, GetAttributes,
    NodeAttributesTreeOperand, NodeMultipleAttributesWithIndexOperand,
    NodeSingleAttributeWithIndexOperand,
};
use edges::{EdgeIndexOperand, EdgeIndicesOperand, EdgeOperand};
use graphrecords_utils::traits::ReadWriteOrPanic;
use group_by::{GroupOperand, GroupedOperand};
use itertools::Itertools;
use nodes::{NodeIndexOperand, NodeIndicesOperand, NodeOperand};
use std::{
    fmt::{Debug, Display},
    hash::Hash,
};
use values::{
    EdgeMultipleValuesWithIndexOperand, EdgeMultipleValuesWithoutIndexOperand,
    EdgeSingleValueWithIndexOperand, GetValues, NodeMultipleValuesWithIndexOperand,
    NodeMultipleValuesWithoutIndexOperand, NodeSingleValueWithIndexOperand,
};

macro_rules! impl_return_operand_for_tuples {
    ($($T:ident),+) => {
        impl<'a, $($T: ReturnOperand<'a>),+> ReturnOperand<'a> for ($($T,)+) {
            type ReturnValue = ($($T::ReturnValue,)+);

            #[allow(non_snake_case)]
            fn evaluate(&self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
                let ($($T,)+) = self;

                $(let $T = $T.evaluate(graphrecord)?;)+

                Ok(($($T,)+))
            }
        }
    };
}

macro_rules! impl_iterator_return_operand {
    ($( $Operand:ident => $Item:ty ),* $(,)?) => {
        $(
            impl<'a> ReturnOperand<'a> for Wrapper<$Operand> {
                type ReturnValue = Box<dyn Iterator<Item = $Item> + 'a>;

                fn evaluate(&self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
                    Ok(Box::new(self.evaluate_backward(graphrecord)?))
                }
            }
        )*
    };
}

macro_rules! impl_direct_return_operand {
    ($( $Operand:ident => $ReturnValue:ty ),* $(,)?) => {
        $(
            impl<'a> ReturnOperand<'a> for Wrapper<$Operand> {
                type ReturnValue = $ReturnValue;

                fn evaluate(&self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
                    self.evaluate_backward(graphrecord)
                }
            }
        )*
    };
}

pub trait Index: Eq + Clone + Hash + Display + GetAttributes {}

impl Index for NodeIndex {}

impl Index for EdgeIndex {}

impl<I: Index> Index for &I {}

pub trait RootOperand:
    GetAllAttributes<Self::Index> + GetValues<Self::Index> + GroupedOperand + Debug + Clone + DeepClone
{
    type Index: Index;
    type Discriminator: Debug + Clone + DeepClone;

    fn _evaluate_forward<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        indices: BoxedIterator<'a, &'a Self::Index>,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a Self::Index>>;

    fn _evaluate_forward_grouped<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        indices: GroupedIterator<'a, BoxedIterator<'a, &'a Self::Index>>,
    ) -> GraphRecordResult<GroupedIterator<'a, BoxedIterator<'a, &'a Self::Index>>>;

    fn _evaluate_backward<'a>(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, &'a Self::Index>>;

    fn _evaluate_backward_grouped_operand<'a>(
        group_operand: &GroupOperand<Self>,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<GroupedIterator<'a, BoxedIterator<'a, &'a Self::Index>>>;

    fn _group_by(&mut self, discriminator: Self::Discriminator) -> Wrapper<GroupOperand<Self>>;

    fn _partition<'a>(
        graphrecord: &'a GraphRecord,
        indices: BoxedIterator<'a, &'a Self::Index>,
        discriminator: &Self::Discriminator,
    ) -> GroupedIterator<'a, BoxedIterator<'a, &'a Self::Index>>;

    fn _merge<'a>(
        indices: GroupedIterator<'a, BoxedIterator<'a, &'a Self::Index>>,
    ) -> BoxedIterator<'a, &'a Self::Index>;
}

impl<'a, O> EvaluateForward<'a> for O
where
    O: RootOperand + 'a,
{
    type InputValue = BoxedIterator<'a, &'a O::Index>;
    type ReturnValue = BoxedIterator<'a, &'a O::Index>;

    fn evaluate_forward(
        &self,
        graphrecord: &'a GraphRecord,
        indices: Self::InputValue,
    ) -> GraphRecordResult<Self::ReturnValue> {
        self._evaluate_forward(graphrecord, indices)
    }
}

pub type GroupedIterator<'a, O> = BoxedIterator<'a, (GroupKey<'a>, O)>;

pub(crate) fn tee_grouped_iterator<'a, O: 'a + Clone>(
    iterator: GroupedIterator<'a, BoxedIterator<'a, O>>,
) -> (
    GroupedIterator<'a, BoxedIterator<'a, O>>,
    GroupedIterator<'a, BoxedIterator<'a, O>>,
) {
    let mut iterators = (Vec::new(), Vec::new());

    iterator.for_each(|(key, inner_iterator)| {
        let (inner_iterator_1, inner_iterator_2) = Itertools::tee(inner_iterator);

        iterators
            .0
            .push((key.clone(), Box::new(inner_iterator_1) as BoxedIterator<_>));
        iterators
            .1
            .push((key, Box::new(inner_iterator_2) as BoxedIterator<_>));
    });

    (
        Box::new(iterators.0.into_iter()),
        Box::new(iterators.1.into_iter()),
    )
}

impl<'a, O> EvaluateForwardGrouped<'a> for O
where
    O: RootOperand + 'a,
{
    fn evaluate_forward_grouped(
        &self,
        graphrecord: &'a GraphRecord,
        indices: GroupedIterator<'a, BoxedIterator<'a, &'a O::Index>>,
    ) -> GraphRecordResult<GroupedIterator<'a, BoxedIterator<'a, &'a O::Index>>> {
        self._evaluate_forward_grouped(graphrecord, indices)
    }
}

impl<'a, O> EvaluateBackward<'a> for O
where
    O: RootOperand + 'a,
{
    type ReturnValue = BoxedIterator<'a, &'a O::Index>;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        self._evaluate_backward(graphrecord)
    }
}

impl<'a, O> EvaluateBackward<'a> for GroupOperand<O>
where
    O: RootOperand + 'a,
{
    type ReturnValue = GroupedIterator<'a, BoxedIterator<'a, &'a O::Index>>;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        O::_evaluate_backward_grouped_operand(self, graphrecord)
    }
}

impl<O: RootOperand> GroupBy for O {
    type Discriminator = <Self as RootOperand>::Discriminator;

    fn group_by(&mut self, discriminator: Self::Discriminator) -> Wrapper<GroupOperand<Self>>
    where
        Self: Sized,
    {
        self._group_by(discriminator)
    }
}

impl<'a, O> PartitionGroups<'a> for O
where
    O: RootOperand + 'a,
{
    type Values = BoxedIterator<'a, &'a O::Index>;

    fn partition(
        graphrecord: &'a GraphRecord,
        indices: Self::Values,
        discriminator: &Self::Discriminator,
    ) -> GroupedIterator<'a, Self::Values> {
        Self::_partition(graphrecord, indices, discriminator)
    }

    fn merge(indices: GroupedIterator<'a, Self::Values>) -> Self::Values {
        Self::_merge(indices)
    }
}

pub trait EvaluateForward<'a> {
    type InputValue;
    type ReturnValue;

    fn evaluate_forward(
        &self,
        graphrecord: &'a GraphRecord,
        values: Self::InputValue,
    ) -> GraphRecordResult<Self::ReturnValue>;
}

impl<'a, O: EvaluateForward<'a>> EvaluateForward<'a> for Wrapper<O> {
    type InputValue = O::InputValue;
    type ReturnValue = O::ReturnValue;

    fn evaluate_forward(
        &self,
        graphrecord: &'a GraphRecord,
        values: Self::InputValue,
    ) -> GraphRecordResult<Self::ReturnValue> {
        self.0.read_or_panic().evaluate_forward(graphrecord, values)
    }
}

pub trait EvaluateForwardGrouped<'a>: EvaluateForward<'a> {
    fn evaluate_forward_grouped(
        &self,
        graphrecord: &'a GraphRecord,
        values: GroupedIterator<'a, Self::InputValue>,
    ) -> GraphRecordResult<GroupedIterator<'a, Self::ReturnValue>>;
}

impl<'a, O: EvaluateForwardGrouped<'a>> EvaluateForwardGrouped<'a> for Wrapper<O> {
    fn evaluate_forward_grouped(
        &self,
        graphrecord: &'a GraphRecord,
        values: GroupedIterator<'a, Self::InputValue>,
    ) -> GraphRecordResult<GroupedIterator<'a, Self::ReturnValue>> {
        self.0
            .read_or_panic()
            .evaluate_forward_grouped(graphrecord, values)
    }
}

pub trait EvaluateBackward<'a> {
    type ReturnValue;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue>;
}

impl<'a, O: EvaluateBackward<'a>> EvaluateBackward<'a> for Wrapper<O> {
    type ReturnValue = O::ReturnValue;

    fn evaluate_backward(
        &self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<Self::ReturnValue> {
        self.0.read_or_panic().evaluate_backward(graphrecord)
    }
}

pub trait ReduceInput<'a>: EvaluateForward<'a> {
    type Context: EvaluateBackward<'a>;

    fn reduce_input(
        &self,
        values: <Self::Context as EvaluateBackward<'a>>::ReturnValue,
    ) -> GraphRecordResult<<Self as EvaluateForward<'a>>::InputValue>;
}

impl<'a, O> Wrapper<O> {
    pub(crate) fn reduce_input(
        &self,
        values: <<O as ReduceInput<'a>>::Context as EvaluateBackward<'a>>::ReturnValue,
    ) -> GraphRecordResult<<O as EvaluateForward<'a>>::InputValue>
    where
        O: ReduceInput<'a>,
    {
        self.0.read_or_panic().reduce_input(values)
    }
}

pub trait DeepClone {
    #[must_use]
    fn deep_clone(&self) -> Self;
}

impl<T: DeepClone> DeepClone for Option<T> {
    fn deep_clone(&self) -> Self {
        self.as_ref().map(DeepClone::deep_clone)
    }
}

impl<T: DeepClone> DeepClone for Box<T> {
    fn deep_clone(&self) -> Self {
        Self::new(T::deep_clone(self))
    }
}

impl<T: DeepClone> DeepClone for Vec<T> {
    fn deep_clone(&self) -> Self {
        self.iter().map(DeepClone::deep_clone).collect()
    }
}

pub(crate) type BoxedIterator<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

#[derive(Debug, Clone)]
pub struct Selection<'a, R: ReturnOperand<'a>> {
    graphrecord: &'a GraphRecord,
    return_operand: R,
}

impl<'a, R: ReturnOperand<'a>> Selection<'a, R> {
    pub fn new_node<Q>(graphrecord: &'a GraphRecord, query: Q) -> Self
    where
        Q: FnOnce(&Wrapper<NodeOperand>) -> R,
    {
        let operand = Wrapper::<NodeOperand>::new(None);

        Self {
            graphrecord,
            return_operand: query(&operand),
        }
    }

    pub fn new_edge<Q>(graphrecord: &'a GraphRecord, query: Q) -> Self
    where
        Q: FnOnce(&Wrapper<EdgeOperand>) -> R,
    {
        let operand = Wrapper::<EdgeOperand>::new(None);

        Self {
            graphrecord,
            return_operand: query(&operand),
        }
    }

    pub fn evaluate(&self) -> GraphRecordResult<R::ReturnValue> {
        self.return_operand.evaluate(self.graphrecord)
    }
}

pub trait ReturnOperand<'a> {
    type ReturnValue;

    fn evaluate(&self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue>;
}

impl_iterator_return_operand!(
    NodeAttributesTreeOperand                 => (&'a NodeIndex, Vec<GraphRecordAttribute>),
    EdgeAttributesTreeOperand                 => (&'a EdgeIndex, Vec<GraphRecordAttribute>),
    NodeMultipleAttributesWithIndexOperand    => (&'a NodeIndex, GraphRecordAttribute),
    NodeMultipleAttributesWithoutIndexOperand => GraphRecordAttribute,
    EdgeMultipleAttributesWithIndexOperand    => (&'a EdgeIndex, GraphRecordAttribute),
    EdgeMultipleAttributesWithoutIndexOperand => GraphRecordAttribute,
    EdgeIndicesOperand                        => EdgeIndex,
    NodeIndicesOperand                        => NodeIndex,
    NodeMultipleValuesWithIndexOperand        => (&'a NodeIndex, GraphRecordValue),
    NodeMultipleValuesWithoutIndexOperand     => GraphRecordValue,
    EdgeMultipleValuesWithIndexOperand        => (&'a EdgeIndex, GraphRecordValue),
    EdgeMultipleValuesWithoutIndexOperand     => GraphRecordValue,
);

impl_direct_return_operand!(
    NodeSingleAttributeWithIndexOperand    => Option<(&'a NodeIndex, GraphRecordAttribute)>,
    NodeSingleAttributeWithoutIndexOperand => Option<GraphRecordAttribute>,
    EdgeSingleAttributeWithIndexOperand    => Option<(&'a EdgeIndex, GraphRecordAttribute)>,
    EdgeSingleAttributeWithoutIndexOperand => Option<GraphRecordAttribute>,
    EdgeIndexOperand                       => Option<EdgeIndex>,
    NodeIndexOperand                       => Option<NodeIndex>,
    NodeSingleValueWithIndexOperand        => Option<(&'a NodeIndex, GraphRecordValue)>,
    NodeSingleValueWithoutIndexOperand     => Option<GraphRecordValue>,
    EdgeSingleValueWithIndexOperand        => Option<(&'a EdgeIndex, GraphRecordValue)>,
    EdgeSingleValueWithoutIndexOperand     => Option<GraphRecordValue>,
);

impl<'a, O: GroupedOperand> ReturnOperand<'a> for Wrapper<GroupOperand<O>>
where
    GroupOperand<O>: EvaluateBackward<'a>,
    Wrapper<O>: ReturnOperand<'a>,
{
    type ReturnValue = <Self as EvaluateBackward<'a>>::ReturnValue;

    fn evaluate(&self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
        self.evaluate_backward(graphrecord)
    }
}

impl_return_operand_for_tuples!(R1);
impl_return_operand_for_tuples!(R1, R2);
impl_return_operand_for_tuples!(R1, R2, R3);
impl_return_operand_for_tuples!(R1, R2, R3, R4);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13);
impl_return_operand_for_tuples!(R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14);
impl_return_operand_for_tuples!(
    R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15
);

impl<'a, R: ReturnOperand<'a>> ReturnOperand<'a> for &R {
    type ReturnValue = R::ReturnValue;

    fn evaluate(&self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
        R::evaluate(self, graphrecord)
    }
}

impl<'a, R: ReturnOperand<'a>> ReturnOperand<'a> for &mut R {
    type ReturnValue = R::ReturnValue;

    fn evaluate(&self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
        R::evaluate(self, graphrecord)
    }
}
