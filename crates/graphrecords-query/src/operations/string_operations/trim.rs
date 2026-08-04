use super::{string_rebuild_map_bare, string_rebuild_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::StringValue,
    element::Preserving,
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Trim,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Trim")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct TrimOperation;

impl Prepare for TrimOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V> ElementKernel<Indexed<I, V>> for TrimOperation
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
        Ok(string_rebuild_map_indexed::<I, V>(
            Self::LABEL,
            |_, value| Ok(value.trim().to_string()),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V> ElementKernel<Bare<V>> for TrimOperation
where
    V: StringValue + BareValueDomain,
{
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_rebuild_map_bare::<V>(Self::LABEL, |_, value| {
            Ok(value.trim().to_string())
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O: Apply<TrimOperation>> Trim for O {
    type ReturnOperand = O::Output;

    fn trim(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), TrimOperation))
    }
}

operation_manifest! {
    TrimOperation {
        method: Trim::trim;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: StringValue>;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: Preserving;
        }
        kernel {
            parameters: <V: StringValue + BareValueDomain>;
            input: Bare<V>;
            output: Bare<V>;
            emission: Preserving;
        }
    }
}
