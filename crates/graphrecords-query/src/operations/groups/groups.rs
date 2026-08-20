use crate::{
    Definite, EntityReference, EvaluateExpression, Explain, IndexDomain, Indexed, Multiple,
    OrderState, QueryResult, Single, Unit, Unordered,
    expressions::GroupsExpression,
    index::GroupMembership,
    operations::{Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::Groups,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::distinct::Distinct;
use std::iter::empty;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Groups")]
#[plan(optimizer_hints(empty = if_any))]
pub struct GroupsOperation;

impl<E: GroupMembership, O: OrderState> LaneKernel<Indexed<E, Unit>, Multiple<O>>
    for GroupsOperation
{
    type Output = GroupsExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, E, Unit, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut groups = Distinct::default();

        for (entity, membership) in values {
            membership?;
            groups.extend(E::group_addresses(graphrecord, &entity));
        }

        Ok(Box::new(groups.into_iter().map(|group| (group, Ok(())))))
    }
}

impl<E: GroupMembership> LaneKernel<Indexed<E, Unit>, Single> for GroupsOperation {
    type Output = GroupsExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, E, Unit, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let Some((entity, membership)) = value else {
            return Ok(Box::new(empty()));
        };
        membership?;

        Ok(Box::new(
            E::group_addresses(graphrecord, &entity).map(|group| (group, Ok(()))),
        ))
    }
}

impl<E: GroupMembership> LaneKernel<Indexed<E, Unit>, Definite> for GroupsOperation {
    type Output = GroupsExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, E, Unit, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let (entity, membership) = value;
        membership?;

        Ok(Box::new(
            E::group_addresses(graphrecord, &entity).map(|group| (group, Ok(()))),
        ))
    }
}

impl<E: GroupMembership, I: IndexDomain, O: OrderState>
    LaneKernel<Indexed<I, EntityReference<E>>, Multiple<O>> for GroupsOperation
{
    type Output = GroupsExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<E>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let mut groups = Distinct::default();

        for value in values {
            let entity = value.1?;
            groups.extend(E::group_addresses(graphrecord, entity.address()));
        }

        Ok(Box::new(groups.into_iter().map(|group| (group, Ok(())))))
    }
}

impl<E: GroupMembership, I: IndexDomain> LaneKernel<Indexed<I, EntityReference<E>>, Single>
    for GroupsOperation
{
    type Output = GroupsExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<E>, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let Some(value) = value else {
            return Ok(Box::new(empty()));
        };
        let entity = value.1?;

        Ok(Box::new(
            E::group_addresses(graphrecord, entity.address()).map(|group| (group, Ok(()))),
        ))
    }
}

impl<E: GroupMembership, I: IndexDomain> LaneKernel<Indexed<I, EntityReference<E>>, Definite>
    for GroupsOperation
{
    type Output = GroupsExpression<Unordered>;

    fn execute<'a>(
        graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<E>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let entity = value.1?;

        Ok(Box::new(
            E::group_addresses(graphrecord, entity.address()).map(|group| (group, Ok(()))),
        ))
    }
}

impl<E: Build<GroupsOperation>> Groups for E {
    type Output = E::Output;

    fn groups(&self) -> Self::Output {
        self.build(GroupsOperation)
    }
}

operation_manifest! {
    GroupsOperation {
        method: Groups::groups;
        scope: lane;

        kernel {
            parameters: <E: GroupMembership, O: OrderState>;
            input: (Indexed<E, Unit>, Multiple<O>);
            output: GroupsExpression<Unordered>;
        }

        kernel {
            parameters: <E: GroupMembership>;
            input: (Indexed<E, Unit>, Single);
            output: GroupsExpression<Unordered>;
        }

        kernel {
            parameters: <E: GroupMembership>;
            input: (Indexed<E, Unit>, Definite);
            output: GroupsExpression<Unordered>;
        }

        kernel {
            parameters: <E: GroupMembership, I: IndexDomain, O: OrderState>;
            input: (Indexed<I, EntityReference<E>>, Multiple<O>);
            output: GroupsExpression<Unordered>;
        }

        kernel {
            parameters: <E: GroupMembership, I: IndexDomain>;
            input: (Indexed<I, EntityReference<E>>, Single);
            output: GroupsExpression<Unordered>;
        }

        kernel {
            parameters: <E: GroupMembership, I: IndexDomain>;
            input: (Indexed<I, EntityReference<E>>, Definite);
            output: GroupsExpression<Unordered>;
        }
    }
}
