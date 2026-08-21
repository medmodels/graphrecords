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
    graphrecord::{EdgeIndex, GroupIndex},
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

impl<O: OrderState> LaneKernel<Indexed<GroupIndex, Unit>, Multiple<O>> for MemberEdgesOperation {
    type Output = EdgesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, GroupIndex, Unit, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut edges = Distinct::default();

        for (group_index, membership) in values {
            membership?;
            edges.extend(EdgeIndex::addresses_in_group(graphrecord, group_index));
        }

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl LaneKernel<Indexed<GroupIndex, Unit>, Single> for MemberEdgesOperation {
    type Output = EdgesExpression<Unordered>;

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
            EdgeIndex::addresses_in_group(graphrecord, group_index).map(|edge| (edge, Ok(()))),
        ))
    }
}

impl LaneKernel<Indexed<GroupIndex, Unit>, Definite> for MemberEdgesOperation {
    type Output = EdgesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, GroupIndex, Unit, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (group_index, membership) = value;
        membership?;

        Ok(Box::new(
            EdgeIndex::addresses_in_group(graphrecord, group_index).map(|edge| (edge, Ok(()))),
        ))
    }
}

impl<I: IndexDomain, O: OrderState> LaneKernel<Indexed<I, EntityReference<GroupIndex>>, Multiple<O>>
    for MemberEdgesOperation
{
    type Output = EdgesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<GroupIndex>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut edges = Distinct::default();

        for value in values {
            let group_index = value.1?;
            edges.extend(EdgeIndex::addresses_in_group(
                graphrecord,
                *group_index.address(),
            ));
        }

        Ok(Box::new(edges.into_iter().map(|edge| (edge, Ok(())))))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<GroupIndex>>, Single>
    for MemberEdgesOperation
{
    type Output = EdgesExpression<Unordered>;

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
            EdgeIndex::addresses_in_group(graphrecord, *group_index.address())
                .map(|edge| (edge, Ok(()))),
        ))
    }
}

impl<I: IndexDomain> LaneKernel<Indexed<I, EntityReference<GroupIndex>>, Definite>
    for MemberEdgesOperation
{
    type Output = EdgesExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<GroupIndex>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let group_index = value.1?;

        Ok(Box::new(
            EdgeIndex::addresses_in_group(graphrecord, *group_index.address())
                .map(|edge| (edge, Ok(()))),
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
            input: (Indexed<GroupIndex, Unit>, Multiple<O>);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<GroupIndex, Unit>, Single);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <>;
            input: (Indexed<GroupIndex, Unit>, Definite);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain, O: OrderState>;
            input: (Indexed<I, EntityReference<GroupIndex>>, Multiple<O>);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<GroupIndex>>, Single);
            output: EdgesExpression<Unordered>;
        }

        kernel {
            parameters: <I: IndexDomain>;
            input: (Indexed<I, EntityReference<GroupIndex>>, Definite);
            output: EdgesExpression<Unordered>;
        }
    }
}
