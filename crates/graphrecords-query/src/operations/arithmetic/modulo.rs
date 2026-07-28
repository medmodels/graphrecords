use super::{arithmetic_bare, arithmetic_indexed};
use crate::value::ValueModulo;
use crate::{
    Bare, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult, ValueType,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Modulo,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Modulo")]
#[plan(optimizer_hints(empty = if_all))]
pub struct ModuloOperation<A> {
    #[argument]
    argument: A,
}

impl<A: Prepare> Prepare for ModuloOperation<A> {
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

impl<I, V, A> ElementKernel<Indexed<I, V>> for ModuloOperation<A>
where
    I: IndexDomain,
    for<'a> V: ValueModulo + ValueType<Value<'a> = <V as ValueType>::Owned>,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(arithmetic_indexed::<_, A, V>(
            prepared,
            Self::LABEL,
            V::modulo,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for ModuloOperation<A>
where
    for<'a> V: ValueModulo + ValueType<Value<'a> = <V as ValueType>::Owned>,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = <V as ValueType>::Owned>,
{
    type Emission = A::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(arithmetic_bare::<A, V>(prepared, Self::LABEL, V::modulo))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<O, A> Modulo<A> for O
where
    ModuloOperation<A>: Operation,
    O: Apply<ModuloOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn modulo(&self, argument: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            ModuloOperation { argument },
        ))
    }
}
