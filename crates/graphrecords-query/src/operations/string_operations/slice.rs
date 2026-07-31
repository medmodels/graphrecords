use super::{string_rebuild_map_bare, string_rebuild_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, Failure, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::StringValue,
    element::Preserving,
    error::string::InvalidStringSlice,
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Slice,
};
use graphrecords_core::GraphRecord;

fn slice_string(label: &'static str, value: &str, start: usize, end: usize) -> QueryResult<String> {
    let characters: Vec<_> = value.chars().collect();

    if start > end || end > characters.len() {
        return Err(Failure::new(
            label,
            InvalidStringSlice::new(start, end, characters.len()),
        ));
    }

    Ok(characters[start..end].iter().collect())
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Slice")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct SliceOperation {
    #[explain(label)]
    start: usize,
    #[explain(label)]
    end: usize,
}

impl Prepare for SliceOperation {
    type Prepared<'a> = (usize, usize);

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((self.start, self.end))
    }
}

impl<I, V> ElementKernel<Indexed<I, V>> for SliceOperation
where
    I: IndexDomain,
    V: StringValue,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_rebuild_map_indexed::<I, V>(
            Self::LABEL,
            move |label, value| slice_string(label, &value, prepared.0, prepared.1),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V> ElementKernel<Bare<V>> for SliceOperation
where
    V: StringValue + BareValueDomain,
{
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_rebuild_map_bare::<V>(
            Self::LABEL,
            move |label, value| slice_string(label, &value, prepared.0, prepared.1),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O: Apply<SliceOperation>> Slice for O {
    type ReturnOperand = O::Output;

    fn slice(&self, start: usize, end: usize) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            SliceOperation { start, end },
        ))
    }
}

operation_manifest! {
    SliceOperation {
        method: Slice::slice;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: StringValue>;
            field: start: usize;
            field: end: usize;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: Preserving;
        }
        kernel {
            parameters: <V: StringValue + BareValueDomain>;
            field: start: usize;
            field: end: usize;
            input: Bare<V>;
            output: Bare<V>;
            emission: Preserving;
        }
    }
}
