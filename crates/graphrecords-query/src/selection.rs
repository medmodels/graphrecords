use crate::{
    BoxedIterator, NodeOperand,
    bool::BoolMaskOperand,
    edges::{AllEdges, EdgeOperand},
    group::{
        Discriminator, GroupOperand, GroupableOperand, GroupedIterator, GroupedOperandContext,
    },
    nodes::AllNodes,
    values::MultipleValuesOperand,
};
use graphrecords_core::{
    GraphRecord,
    errors::GraphRecordResult,
    graphrecord::{EdgeIndex, GraphRecordValue, NodeIndex},
};
use std::sync::Arc;

macro_rules! impl_iterator_return_operand {
    ($( $Operand:ty => $Item:ty ),* $(,)?) => {
        $(
            impl<'a> ReturnOperand<'a> for $Operand {
                type ReturnValue = BoxedIterator<'a, $Item>;

                fn evaluate(&'a self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
                    self.evaluate(graphrecord)
                }
            }
        )*
    };
}

macro_rules! impl_return_operand_for_tuples {
    ($($T:ident),+) => {
        impl<'a, $($T: ReturnOperand<'a>),+> ReturnOperand<'a> for ($($T,)+) {
            type ReturnValue = ($($T::ReturnValue,)+);

            #[allow(non_snake_case)]
            fn evaluate(&'a self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
                let ($($T,)+) = self;

                $(let $T = $T.evaluate(graphrecord)?;)+

                Ok(($($T,)+))
            }
        }
    };
}

#[derive(Debug, Clone)]
pub struct Selection<'a, R: ReturnOperand<'a>> {
    graphrecord: &'a GraphRecord,
    return_operand: R,
}

impl<'a, R: ReturnOperand<'a>> Selection<'a, R> {
    pub fn new_node<Q>(graphrecord: &'a GraphRecord, query: Q) -> Self
    where
        Q: FnOnce(&NodeOperand) -> R,
    {
        let operand = NodeOperand::new(AllNodes);

        Self {
            graphrecord,
            return_operand: query(&operand),
        }
    }

    pub fn new_edge<Q>(graphrecord: &'a GraphRecord, query: Q) -> Self
    where
        Q: FnOnce(&EdgeOperand) -> R,
    {
        let operand = EdgeOperand::new(AllEdges);

        Self {
            graphrecord,
            return_operand: query(&operand),
        }
    }

    pub fn evaluate(&'a self) -> GraphRecordResult<R::ReturnValue> {
        self.return_operand.evaluate(self.graphrecord)
    }
}

pub trait ReturnOperand<'a> {
    type ReturnValue;

    fn evaluate(&'a self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue>;
}

impl_iterator_return_operand!(
    MultipleValuesOperand<NodeOperand> => (&'a NodeIndex, GraphRecordValue),
    MultipleValuesOperand<EdgeOperand> => (&'a EdgeIndex, GraphRecordValue),
    BoolMaskOperand<NodeOperand>       => (&'a NodeIndex, bool),
    BoolMaskOperand<EdgeOperand>       => (&'a EdgeIndex, bool),
);

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

    fn evaluate(&'a self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
        R::evaluate(self, graphrecord)
    }
}

impl<'a, R: ReturnOperand<'a>> ReturnOperand<'a> for &mut R {
    type ReturnValue = R::ReturnValue;

    fn evaluate(&'a self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
        R::evaluate(self, graphrecord)
    }
}

impl<'a, O, D> ReturnOperand<'a> for GroupOperand<O, D>
where
    D: Discriminator,
    O: GroupableOperand + ReturnOperand<'a>,
    Arc<dyn GroupedOperandContext<O, D>>: 'a,
{
    type ReturnValue = GroupedIterator<'a, D::Key<'a>, BoxedIterator<'a, O::Output<'a>>>;

    fn evaluate(&'a self, graphrecord: &'a GraphRecord) -> GraphRecordResult<Self::ReturnValue> {
        Self::evaluate(self, graphrecord)
    }
}
