use crate::{
    Definite, EntityReference, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple,
    OrderState, QueryResult, Single, Unit, Unordered,
    expressions::NodesExpression,
    index::GroupMembership,
    operations::{Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::Nodes,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GroupIndex, NodeIndex, StateView},
};
use graphrecords_utils::distinct::Distinct;
use std::iter::empty;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Nodes")]
#[plan(optimizer_hints(empty = if_any))]
pub struct NodesOperation;

impl<O: OrderState> LaneKernel<Indexed<EdgeIndex, Unit>, Multiple<O>> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, EdgeIndex, Unit, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let state = StateView::of(graphrecord);
        let mut nodes = Distinct::default();

        for (edge, membership) in values {
            membership?;
            let (source, target) = state.edge_endpoints(edge);

            nodes.extend([source, target]);
        }

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl LaneKernel<Indexed<EdgeIndex, Unit>, Single> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, EdgeIndex, Unit, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let Some((edge, membership)) = value else {
            return Ok(Box::new(empty()));
        };
        membership?;

        let (source, target) = StateView::of(graphrecord).edge_endpoints(edge);
        let nodes: Vec<_> = [source, target].into_iter().collect::<Distinct<_>>().into();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl LaneKernel<Indexed<EdgeIndex, Unit>, Definite> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, EdgeIndex, Unit, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (edge, membership) = value;
        membership?;

        let (source, target) = StateView::of(graphrecord).edge_endpoints(edge);
        let nodes: Vec<_> = [source, target].into_iter().collect::<Distinct<_>>().into();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, EntityReference<EdgeIndex>>, Multiple<O>>
    for NodesOperation
{
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<EdgeIndex>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let state = StateView::of(graphrecord);
        let mut nodes = Distinct::default();

        for value in values {
            let edge = value.1?;
            let (source, target) = state.edge_endpoints(*edge.address());

            nodes.extend([source, target]);
        }

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<EdgeIndex>>, Single> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<EdgeIndex>, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let Some(value) = value else {
            return Ok(Box::new(empty()));
        };
        let edge = value.1?;
        let (source, target) = StateView::of(graphrecord).edge_endpoints(*edge.address());
        let nodes: Vec<_> = [source, target].into_iter().collect::<Distinct<_>>().into();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<EdgeIndex>>, Definite>
    for NodesOperation
{
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<EdgeIndex>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let edge = value.1?;
        let (source, target) = StateView::of(graphrecord).edge_endpoints(*edge.address());
        let nodes: Vec<_> = [source, target].into_iter().collect::<Distinct<_>>().into();

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<O: OrderState> LaneKernel<Indexed<GroupIndex, Unit>, Multiple<O>> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, GroupIndex, Unit, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut nodes = Distinct::default();

        for (group_index, membership) in values {
            membership?;
            nodes.extend(NodeIndex::addresses_in_group(graphrecord, group_index));
        }

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl LaneKernel<Indexed<GroupIndex, Unit>, Single> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, GroupIndex, Unit, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let Some((group_index, membership)) = value else {
            return Ok(Box::new(empty()));
        };
        membership?;

        Ok(Box::new(
            NodeIndex::addresses_in_group(graphrecord, group_index).map(|node| (node, Ok(()))),
        ))
    }
}

impl LaneKernel<Indexed<GroupIndex, Unit>, Definite> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, GroupIndex, Unit, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (group_index, membership) = value;
        membership?;

        Ok(Box::new(
            NodeIndex::addresses_in_group(graphrecord, group_index).map(|node| (node, Ok(()))),
        ))
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, EntityReference<GroupIndex>>, Multiple<O>>
    for NodesOperation
{
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<GroupIndex>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut nodes = Distinct::default();

        for value in values {
            let group_index = value.1?;
            nodes.extend(NodeIndex::addresses_in_group(
                graphrecord,
                *group_index.address(),
            ));
        }

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<GroupIndex>>, Single>
    for NodesOperation
{
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<GroupIndex>, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let Some(value) = value else {
            return Ok(Box::new(empty()));
        };
        let group_index = value.1?;

        Ok(Box::new(
            NodeIndex::addresses_in_group(graphrecord, *group_index.address())
                .map(|node| (node, Ok(()))),
        ))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<GroupIndex>>, Definite>
    for NodesOperation
{
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<GroupIndex>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let group_index = value.1?;

        Ok(Box::new(
            NodeIndex::addresses_in_group(graphrecord, *group_index.address())
                .map(|node| (node, Ok(()))),
        ))
    }
}

impl<E: Build<NodesOperation>> Nodes for E {
    type Output = E::Output;

    fn nodes(&self) -> Self::Output {
        self.build(NodesOperation)
    }
}

operation_manifest! {
    NodesOperation {
        method: Nodes::nodes;
        scope: lane;

        kernel {
            parameters: <O: OrderState>;
            input: (Indexed<EdgeIndex, Unit>, Multiple<O>);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<EdgeIndex, Unit>, Single);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<EdgeIndex, Unit>, Definite);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain, O: OrderState>;
            input: (Indexed<I, EntityReference<EdgeIndex>>, Multiple<O>);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<EdgeIndex>>, Single);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<EdgeIndex>>, Definite);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <O: OrderState>;
            input: (Indexed<GroupIndex, Unit>, Multiple<O>);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<GroupIndex, Unit>, Single);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<GroupIndex, Unit>, Definite);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain, O: OrderState>;
            input: (Indexed<I, EntityReference<GroupIndex>>, Multiple<O>);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<GroupIndex>>, Single);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<GroupIndex>>, Definite);
            output: NodesExpression<Unordered>;
        }
    }
}
