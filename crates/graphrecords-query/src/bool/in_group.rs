use crate::{
    BoxedIterator, EdgeOperand, RootOperand,
    bool::{BoolMaskContext, BoolMaskOperand, BoolMaskOperandContext},
    nodes::NodeOperand,
    traits::InGroup,
};
use graphrecords_core::{GraphRecord, errors::GraphRecordResult, graphrecord::Group};
use graphrecords_utils::aliases::GrHashSet;

pub struct InGroupContext<O: RootOperand> {
    source: O,
    group: Group,
    lookup: Box<dyn GroupLookup<O>>,
}

impl<O: RootOperand> From<InGroupContext<O>> for BoolMaskContext<O> {
    fn from(context: InGroupContext<O>) -> Self {
        Self::InGroup(context)
    }
}

impl<O: RootOperand> BoolMaskOperandContext for InGroupContext<O> {
    type Operand = O;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
    ) -> GraphRecordResult<BoxedIterator<'a, (<Self::Operand as RootOperand>::Index<'a>, bool)>>
    {
        self.lookup.evaluate(graphrecord, &self.source, &self.group)
    }
}

pub trait GroupLookup<O: RootOperand>: Send + Sync {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        source: &'a O,
        group: &Group,
    ) -> GraphRecordResult<BoxedIterator<'a, (<O as RootOperand>::Index<'a>, bool)>>;
}

struct NodeGroupLookup;

impl GroupLookup<NodeOperand> for NodeGroupLookup {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        source: &'a NodeOperand,
        group: &Group,
    ) -> GraphRecordResult<BoxedIterator<'a, (<NodeOperand as RootOperand>::Index<'a>, bool)>> {
        let node_indices = source.evaluate(graphrecord)?;

        let node_indices_in_group: GrHashSet<_> = graphrecord.nodes_in_group(group)?.collect();

        Ok(Box::new(node_indices.map(move |node_index| {
            let in_group = node_indices_in_group.contains(&node_index);

            (node_index, in_group)
        })))
    }
}

impl InGroup for NodeOperand {
    type ReturnOperand = BoolMaskOperand<Self>;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        BoolMaskOperand::new(InGroupContext {
            source: self.clone(),
            group,
            lookup: Box::new(NodeGroupLookup),
        })
    }
}

struct EdgeGroupLookup;

impl GroupLookup<EdgeOperand> for EdgeGroupLookup {
    fn evaluate<'a>(
        &self,
        graphrecord: &'a GraphRecord,
        source: &'a EdgeOperand,
        group: &Group,
    ) -> GraphRecordResult<BoxedIterator<'a, (<EdgeOperand as RootOperand>::Index<'a>, bool)>> {
        let edge_indices = source.evaluate(graphrecord)?;

        let edge_indices_in_group: GrHashSet<_> = graphrecord.edges_in_group(group)?.collect();

        Ok(Box::new(edge_indices.map(move |edge_index| {
            let in_group = edge_indices_in_group.contains(&edge_index);

            (edge_index, in_group)
        })))
    }
}

impl InGroup for EdgeOperand {
    type ReturnOperand = BoolMaskOperand<Self>;

    fn in_group(&self, group: Group) -> Self::ReturnOperand {
        BoolMaskOperand::new(InGroupContext {
            source: self.clone(),
            group,
            lookup: Box::new(EdgeGroupLookup),
        })
    }
}
