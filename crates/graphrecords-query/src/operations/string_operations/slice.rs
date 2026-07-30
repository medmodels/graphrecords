use super::{string_map_bare, string_map_indexed};
use crate::{
    Bare, Explain, Failure, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    capabilities::StringValue,
    element::Preserving,
    error::string::InvalidStringSlice,
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
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
#[plan(optimizer_hints(empty = if_any))]
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
        Ok(string_map_indexed::<I, V, V>(
            Self::LABEL,
            move |label, value| {
                slice_string(label, &value, prepared.0, prepared.1).map(V::from_string)
            },
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V> ElementKernel<Bare<V>> for SliceOperation
where
    V: StringValue,
{
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_map_bare::<V, V>(Self::LABEL, move |label, value| {
            slice_string(label, &value, prepared.0, prepared.1).map(V::from_string)
        }))
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
