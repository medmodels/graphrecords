use super::{arithmetic_bare, arithmetic_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::ValueAdd,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::Add,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Add")]
#[plan(optimizer_hints(empty = if_all))]
pub struct AddOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for AddOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.argument.prepare(graphrecord, cache)
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for AddOperation<A>
where
    I: IndexDomain,
    V: ValueAdd,
    A: ArgumentSource<Keyed<I>, V>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(arithmetic_indexed::<_, V, A>(prepared, Self::LABEL, V::add))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, A> ElementKernel<Bare<V>> for AddOperation<A>
where
    V: ValueAdd + BareValueDomain,
    A: ArgumentSource<Unaligned, V>,
{
    type Emission = A::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(arithmetic_bare::<V, A>(prepared, Self::LABEL, V::add))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O, A> Add<A> for O
where
    AddOperation<A>: Operation,
    O: Apply<AddOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn add(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            AddOperation { argument },
        ))
    }
}

operation_manifest! {
    AddOperation<A> {
        method: Add<A>::add;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueAdd>;
            argument: A: ArgumentSource<Keyed<I>, V>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueAdd + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned, V>;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
