use super::{string_rebuild_map_bare, string_rebuild_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, Failure, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueString,
    element::Preserving,
    error::string::InvalidStringSlice,
    execution::EvaluationCache,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Slice,
};
use graphrecords_core::GraphRecord;

fn slice_string(value: &str, start: usize, end: usize, label: &'static str) -> QueryResult<String> {
    let characters: Vec<_> = value.chars().collect();

    if start > end || end > characters.len() {
        return Err(Failure::new(
            InvalidStringSlice::new(start, end, characters.len()),
            label,
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
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((self.start, self.end))
    }
}

impl<I: IndexDomain, V: ValueString> ElementKernel<Indexed<I, V>> for SliceOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_rebuild_map_indexed::<I, V>(
            graphrecord,
            move |value, label| slice_string(value, prepared.0, prepared.1, label),
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V: ValueString + BareValueDomain> ElementKernel<Bare<V>> for SliceOperation {
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_rebuild_map_bare::<V>(
            move |value, label| slice_string(value, prepared.0, prepared.1, label),
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E: Build<SliceOperation>> Slice for E {
    type Output = E::Output;

    fn slice(&self, start: usize, end: usize) -> Self::Output {
        self.build(SliceOperation { start, end })
    }
}

operation_manifest! {
    SliceOperation {
        method: Slice::slice;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueString>;
            field: start: usize;
            field: end: usize;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: Preserving;
        }

        kernel {
            parameters: <V: ValueString + BareValueDomain>;
            field: start: usize;
            field: end: usize;
            input: Bare<V>;
            output: Bare<V>;
            emission: Preserving;
        }
    }
}
