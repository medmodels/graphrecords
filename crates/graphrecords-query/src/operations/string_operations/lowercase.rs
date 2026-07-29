use super::{string_map_bare, string_map_indexed};
use crate::{
    Bare, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    element::Preserving,
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::Lowercase,
    value::StringValue,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Lowercase")]
#[plan(optimizer_hints(empty = if_any))]
pub struct LowercaseOperation;

impl Prepare for LowercaseOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V> ElementKernel<Indexed<I, V>> for LowercaseOperation
where
    I: IndexDomain,
    V: StringValue,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_map_indexed::<I, V, V>(Self::LABEL, |_, value| {
            Ok(V::from_string(value.to_lowercase()))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<V> ElementKernel<Bare<V>> for LowercaseOperation
where
    V: StringValue,
{
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_map_bare::<V, V>(Self::LABEL, |_, value| {
            Ok(V::from_string(value.to_lowercase()))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            distinct: None,
            ..input
        }
    }
}

impl<O: Apply<LowercaseOperation>> Lowercase for O {
    type ReturnOperand = O::Output;

    fn lowercase(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), LowercaseOperation))
    }
}
