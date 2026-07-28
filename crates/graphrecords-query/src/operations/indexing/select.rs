use crate::{
    Bare, Definite, EntityDomain, EntityReference, EvaluateOperand, Explain, IndexDomain, Indexed,
    Multiple, Operand, OrderState, QueryResult, Single, Unordered,
    execution::EvaluationCache,
    operands::{DefiniteElementOperand, ElementOperand, ElementsOperand},
    operations::{
        Apply, BareStream, KeyedStream, LaneKernel, Operation, OperationContext, Prepare,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Select,
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Lane)]
#[explain(label = "Select")]
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

impl Prepare for SelectOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<E: EntityDomain, I: IndexDomain, O: OrderState>
    LaneKernel<Indexed<I, EntityReference<E>>, Multiple<O>> for SelectOperation
{
    type Output = ElementsOperand<E, Unordered>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: KeyedStream<'a, I, EntityReference<E>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let targets: GrHashSet<_> = values
            .map(|(_, reference)| reference)
            .collect::<QueryResult<_>>()?;

        Ok(Box::new(targets.into_iter().map(|target| (target, Ok(())))))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        multiple_estimate(&input)
    }
}

impl<E: EntityDomain, I: IndexDomain> LaneKernel<Indexed<I, EntityReference<E>>, Single>
    for SelectOperation
{
    type Output = ElementOperand<E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<E>, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let Some((_, reference)) = value else {
            return Ok(None);
        };
        let target = reference?;

        Ok(Some((target, Ok(()))))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<E: EntityDomain, I: IndexDomain> LaneKernel<Indexed<I, EntityReference<E>>, Definite>
    for SelectOperation
{
    type Output = DefiniteElementOperand<E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: KeyedStream<'a, I, EntityReference<E>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let target = value.1?;

        Ok((target, Ok(())))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<E: EntityDomain, O: OrderState> LaneKernel<Bare<EntityReference<E>>, Multiple<O>>
    for SelectOperation
{
    type Output = ElementsOperand<E, Unordered>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        values: BareStream<'a, EntityReference<E>, Multiple<O>>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        let targets: GrHashSet<_> = values.collect::<QueryResult<_>>()?;

        Ok(Box::new(targets.into_iter().map(|target| (target, Ok(())))))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        multiple_estimate(&input)
    }
}

impl<E: EntityDomain> LaneKernel<Bare<EntityReference<E>>, Single> for SelectOperation {
    type Output = ElementOperand<E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, EntityReference<E>, Single>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok(value.transpose()?.map(|target| (target, Ok(()))))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        single_estimate(&input)
    }
}

impl<E: EntityDomain> LaneKernel<Bare<EntityReference<E>>, Definite> for SelectOperation {
    type Output = DefiniteElementOperand<E>;

    fn execute<'a>(
        _graphrecord: &'a GraphRecord,
        value: BareStream<'a, EntityReference<E>, Definite>,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<<Self::Output as EvaluateOperand>::ReturnValue<'a>> {
        Ok((value?, Ok(())))
    }

    fn estimate(&self, _input: Estimate, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl<O: Apply<SelectOperation>> Select for O {
    type ReturnOperand = O::Output;

    fn select(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), SelectOperation))
    }
}
