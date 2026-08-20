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
    graphrecord::{EdgeIndex, Group, NodeIndex, StateView},
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

impl<O: OrderState> LaneKernel<Indexed<Group, Unit>, Multiple<O>> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, Group, Unit, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut nodes = Distinct::default();

        for (group, membership) in values {
            membership?;
            nodes.extend(NodeIndex::addresses_in_group(graphrecord, group));
        }

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl LaneKernel<Indexed<Group, Unit>, Single> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, Group, Unit, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let Some((group, membership)) = value else {
            return Ok(Box::new(empty()));
        };
        membership?;

        Ok(Box::new(
            NodeIndex::addresses_in_group(graphrecord, group).map(|node| (node, Ok(()))),
        ))
    }
}

impl LaneKernel<Indexed<Group, Unit>, Definite> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, Group, Unit, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (group, membership) = value;
        membership?;

        Ok(Box::new(
            NodeIndex::addresses_in_group(graphrecord, group).map(|node| (node, Ok(()))),
        ))
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, EntityReference<Group>>, Multiple<O>>
    for NodesOperation
{
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<Group>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut nodes = Distinct::default();

        for value in values {
            let group = value.1?;
            nodes.extend(NodeIndex::addresses_in_group(graphrecord, *group.address()));
        }

        Ok(Box::new(nodes.into_iter().map(|node| (node, Ok(())))))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<Group>>, Single> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<Group>, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let Some(value) = value else {
            return Ok(Box::new(empty()));
        };
        let group = value.1?;

        Ok(Box::new(
            NodeIndex::addresses_in_group(graphrecord, *group.address()).map(|node| (node, Ok(()))),
        ))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<Group>>, Definite> for NodesOperation {
    type Output = NodesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<Group>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let group = value.1?;

        Ok(Box::new(
            NodeIndex::addresses_in_group(graphrecord, *group.address()).map(|node| (node, Ok(()))),
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
            input: (Indexed<Group, Unit>, Multiple<O>);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<Group, Unit>, Single);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<Group, Unit>, Definite);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain, O: OrderState>;
            input: (Indexed<I, EntityReference<Group>>, Multiple<O>);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<Group>>, Single);
            output: NodesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<Group>>, Definite);
            output: NodesExpression<Unordered>;
        }
    }
}
