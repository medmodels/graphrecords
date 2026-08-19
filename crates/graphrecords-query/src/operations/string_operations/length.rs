use super::{string_map_bare, string_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, Failure, IndexDomain, Indexed, Labeled, Operand, QueryResult,
    Scalar,
    capabilities::StringValue,
    element::Preserving,
    error::string::StringLengthOverflow,
    execution::EvaluationCache,
    operations::{Apply, ElementKernel, ElementPipeline, Operation, OperationContext, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Length,
};
use graphrecords_core::{GraphRecord, graphrecord::Value};

pub(super) fn length_chars(label: &'static str, value: &str) -> QueryResult<Value> {
    let length = value.chars().count();
    let length = i64::try_from(length)
        .map_err(|_| Failure::new(label, StringLengthOverflow::new(length)))?;

    Ok(Value::Int(length))
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Length")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct LengthOperation;

impl Prepare for LengthOperation {
    type Prepared<'a> = ();

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(())
    }
}

impl<I, V> ElementKernel<Indexed<I, V>> for LengthOperation
where
    I: IndexDomain,
    V: StringValue,
{
    type Emission = Preserving;
    type OutShape = Indexed<I, Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_map_indexed::<I, V, Scalar>(
            Self::LABEL,
            |label, value| length_chars(label, &value),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V> ElementKernel<Bare<V>> for LengthOperation
where
    V: StringValue + BareValueDomain,
{
    type Emission = Preserving;
    type OutShape = Bare<Scalar>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        _prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_map_bare::<V, Scalar>(Self::LABEL, |label, value| {
            length_chars(label, &value)
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O: Apply<LengthOperation>> Length for O {
    type ReturnOperand = O::Output;

    fn length(&self) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(self.clone(), LengthOperation))
    }
}

operation_manifest! {
    LengthOperation {
        method: Length::length;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: StringValue>;
            input: Indexed<I, V>;
            output: Indexed<I, Scalar>;
            emission: Preserving;
        }
        kernel {
            parameters: <V: StringValue + BareValueDomain>;
            input: Bare<V>;
            output: Bare<Scalar>;
            emission: Preserving;
        }
    }
}
