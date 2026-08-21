use super::{string_map_bare, string_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, Failure, IndexDomain, Indexed, Labeled, QueryResult, Scalar,
    capabilities::ValueString,
    element::Preserving,
    error::string::StringLengthOverflow,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Length,
};
use graphrecords_core::{GraphRecord, graphrecord::ValueView};

fn length_chars<'a>(value: &str, label: &'static str) -> QueryResult<ValueView<'a>> {
    let length = value.chars().count();
    let length = i64::try_from(length)
        .map_err(|_| Failure::new(StringLengthOverflow::new(length), label))?;

    Ok(ValueView::Int(length))
}

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Length")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct LengthOperation;

impl<I: IndexDomain, V: ValueString> ElementKernel<Indexed<I, V>> for LengthOperation {
    type Emission = Preserving;
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_map_indexed::<I, V, Scalar>(
            graphrecord,
            length_chars,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V: ValueString + BareValueDomain> ElementKernel<Bare<V>> for LengthOperation {
    type Emission = Preserving;
    type OutShape = Bare<Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_map_bare::<V, Scalar>(length_chars, Self::LABEL))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E: Build<LengthOperation>> Length for E {
    type Output = E::Output;

    fn length(&self) -> Self::Output {
        self.build(LengthOperation)
    }
}

operation_manifest! {
    LengthOperation {
        method: Length::length;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueString>;
            input: Indexed<I, V>;
            output: Indexed<I, Scalar>;
            emission: Preserving;
        }

        kernel {
            parameters: <V: ValueString + BareValueDomain>;
            input: Bare<V>;
            output: Bare<Scalar>;
            emission: Preserving;
        }
    }
}
