use crate::{
    Bare, Definite, EntityIndexDomain, EntityReference, EvaluateExpression, Explain, IndexDomain,
    Indexed, Multiple, OrderState, QueryResult, Single, Unordered,
    expressions::{DefiniteElementExpression, ElementExpression, ElementsExpression},
    operations::{BareStream, Build, KeyedStream, LaneKernel, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Select,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::distinct::Distinct;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Lane)]
#[explain(label = "Select")]
#[plan(optimizer_hints(empty = if_any))]
pub struct SelectOperation;

const fn multiple_estimate(input: &Estimate) -> Estimate {
    Estimate {
        elements: input.distinct,
        distinct: input.distinct,
        selectivity: None,
        per_group: None,
    }
}

const fn single_estimate(input: &Estimate) -> Estimate {
    Estimate {
        elements: input.elements,
        distinct: input.elements,
        selectivity: None,
        per_group: None,
    }
}

impl<E: EntityIndexDomain, I: IndexDomain, O: OrderState>
    LaneKernel<Indexed<I, EntityReference<E>>, Multiple<O>> for SelectOperation
{
    type Output = ElementsExpression<E, Unordered>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<E>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let targets: Vec<_> = values
            .map(|value| value.1.map(|reference| reference.address().clone()))
            .collect::<QueryResult<Distinct<_>>>()?
            .into();

        Ok(Box::new(targets.into_iter().map(|target| (target, Ok(())))))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        multiple_estimate(&input)
    }
}

impl<E: EntityIndexDomain, I: IndexDomain> LaneKernel<Indexed<I, EntityReference<E>>, Single>
    for SelectOperation
{
    type Output = ElementExpression<E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<E>, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let Some((_, reference)) = value else {
            return Ok(None);
        };
        let target = reference?.address().clone();

        Ok(Some((target, Ok(()))))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<E: EntityIndexDomain, I: IndexDomain> LaneKernel<Indexed<I, EntityReference<E>>, Definite>
    for SelectOperation
{
    type Output = DefiniteElementExpression<E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<E>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let target = value.1?.address().clone();

        Ok((target, Ok(())))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<E: EntityIndexDomain, O: OrderState> LaneKernel<Bare<EntityReference<E>>, Multiple<O>>
    for SelectOperation
{
    type Output = ElementsExpression<E, Unordered>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, EntityReference<E>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        let targets: Vec<_> = values
            .map(|value| value.map(|reference| reference.address().clone()))
            .collect::<QueryResult<Distinct<_>>>()?
            .into();

        Ok(Box::new(targets.into_iter().map(|target| (target, Ok(())))))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        multiple_estimate(&input)
    }
}

impl<E: EntityIndexDomain> LaneKernel<Bare<EntityReference<E>>, Single> for SelectOperation {
    type Output = ElementExpression<E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, EntityReference<E>, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok(value
            .transpose()?
            .map(|target| (target.address().clone(), Ok(()))))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<E: EntityIndexDomain> LaneKernel<Bare<EntityReference<E>>, Definite> for SelectOperation {
    type Output = DefiniteElementExpression<E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, EntityReference<E>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateExpression>::ReturnValue<'a>> {
        Ok((value?.address().clone(), Ok(())))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<E: Build<SelectOperation>> Select for E {
    type Output = E::Output;

    fn select(&self) -> Self::Output {
        self.build(SelectOperation)
    }
}

operation_manifest! {
    SelectOperation {
        method: Select::select;
        scope: lane;

        kernel {
            parameters: <E: EntityIndexDomain, I: IndexDomain, O: OrderState>;
            input: (Indexed<I, EntityReference<E>>, Multiple<O>);
            output: ElementsExpression<E, Unordered>;
        }

        kernel {
            parameters: <E: EntityIndexDomain, I: IndexDomain>;
            input: (Indexed<I, EntityReference<E>>, Single);
            output: ElementExpression<E>;
        }

        kernel {
            parameters: <E: EntityIndexDomain, I: IndexDomain>;
            input: (Indexed<I, EntityReference<E>>, Definite);
            output: DefiniteElementExpression<E>;
        }

        kernel {
            parameters: <E: EntityIndexDomain, O: OrderState>;
            input: (Bare<EntityReference<E>>, Multiple<O>);
            output: ElementsExpression<E, Unordered>;
        }

        kernel {
            parameters: <E: EntityIndexDomain>;
            input: (Bare<EntityReference<E>>, Single);
            output: ElementExpression<E>;
        }

        kernel {
            parameters: <E: EntityIndexDomain>;
            input: (Bare<EntityReference<E>>, Definite);
            output: DefiniteElementExpression<E>;
        }
    }
}
