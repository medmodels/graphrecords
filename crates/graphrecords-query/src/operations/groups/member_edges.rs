use crate::{
    Definite, EntityReference, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple,
    OrderState, QueryResult, Single, Unit, Unordered,
    expressions::EdgesExpression,
    index::GroupMembership,
    operations::{Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::MemberEdges,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, Group},
};
use graphrecords_utils::distinct::Distinct;
use std::iter::empty;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "MemberEdges")]
#[plan(optimizer_hints(empty = if_any))]
pub struct MemberEdgesOperation;

impl<O: OrderState> LaneKernel<Indexed<Group, Unit>, Multiple<O>> for MemberEdgesOperation {
    type Output = EdgesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, Group, Unit, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut edges = Distinct::default();

        for (group, membership) in values {
            membership?;
            edges.extend(EdgeIndex::addresses_in_group(graphrecord, group));
        }

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl LaneKernel<Indexed<Group, Unit>, Single> for MemberEdgesOperation {
    type Output = EdgesExpression<Unordered>;

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
            EdgeIndex::addresses_in_group(graphrecord, group).map(|edge| (edge, Ok(()))),
        ))
    }
}

impl LaneKernel<Indexed<Group, Unit>, Definite> for MemberEdgesOperation {
    type Output = EdgesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, Group, Unit, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (group, membership) = value;
        membership?;

        Ok(Box::new(
            EdgeIndex::addresses_in_group(graphrecord, group).map(|edge| (edge, Ok(()))),
        ))
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, EntityReference<Group>>, Multiple<O>>
    for MemberEdgesOperation
{
    type Output = EdgesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<Group>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut edges = Distinct::default();

        for value in values {
            let group = value.1?;
            edges.extend(EdgeIndex::addresses_in_group(graphrecord, *group.address()));
        }

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<Group>>, Single>
    for MemberEdgesOperation
{
    type Output = EdgesExpression<Unordered>;

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
            EdgeIndex::addresses_in_group(graphrecord, *group.address()).map(|edge| (edge, Ok(()))),
        ))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<Group>>, Definite>
    for MemberEdgesOperation
{
    type Output = EdgesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<Group>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let group = value.1?;

        Ok(Box::new(
            EdgeIndex::addresses_in_group(graphrecord, *group.address()).map(|edge| (edge, Ok(()))),
        ))
    }
}

impl<E: Build<MemberEdgesOperation>> MemberEdges for E {
    type Output = E::Output;

    fn edges(&self) -> Self::Output {
        self.build(MemberEdgesOperation)
    }
}

operation_manifest! {
    MemberEdgesOperation {
        method: MemberEdges::edges;
        scope: lane;

        kernel {
            parameters: <O: OrderState>;
            input: (Indexed<Group, Unit>, Multiple<O>);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<Group, Unit>, Single);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<Group, Unit>, Definite);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain, O: OrderState>;
            input: (Indexed<I, EntityReference<Group>>, Multiple<O>);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<Group>>, Single);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<Group>>, Definite);
            output: EdgesExpression<Unordered>;
        }
    }
}
