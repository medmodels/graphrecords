use super::{padding_character, string_pad_bare, string_pad_indexed};
use crate::{
    Bare, BareValueDomain, Explain, External, Failure, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueString,
    error::string::StringPaddingOverflow,
    execution::EvaluationCache,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::PadStart,
};
use graphrecords_core::GraphRecord;
use std::iter::repeat_n;

fn pad(value: &str, width: usize, character: &str, label: &'static str) -> QueryResult<String> {
    let character = padding_character(character, label)?;
    let padding_length = width.saturating_sub(value.chars().count());
    if padding_length == 0 {
        return Ok(value.to_string());
    }

    let capacity = padding_length
        .checked_mul(character.len_utf8())
        .and_then(|padding_bytes| padding_bytes.checked_add(value.len()))
        .ok_or_else(|| Failure::new(StringPaddingOverflow::new(width), label))?;
    let mut padded = String::new();
    padded
        .try_reserve(capacity)
        .map_err(|error| Failure::new(External::new(error), label))?;
    padded.extend(repeat_n(character, padding_length));
    padded.push_str(value);

    Ok(padded)
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "PadStart")]
#[plan(optimizer_hints(empty = if_all))]
pub struct PadStartOperation<A> {
    #[explain(label)]
    width: usize,
    #[argument]
    character: A,
}

impl<A: Prepare> Prepare for PadStartOperation<A> {
    type Prepared<'a>
        = (usize, A::Prepared<'a>)
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((self.width, self.character.prepare(graphrecord, cache)?))
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for PadStartOperation<A>
where
    I: IndexDomain,
    V: ValueString,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: ValueString,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_pad_indexed::<_, V, A>(
            graphrecord,
            prepared,
            pad,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, A> ElementKernel<Bare<V>> for PadStartOperation<A>
where
    V: ValueString + BareValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: ValueString,
{
    type Emission = A::Retention;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_pad_bare::<V, A>(
            graphrecord,
            prepared,
            pad,
            Self::LABEL,
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E, A> PadStart<A> for E
where
    PadStartOperation<A>: Operation,
    E: Build<PadStartOperation<A>>,
{
    type Output = E::Output;

    fn pad_start(&self, width: usize, character: A) -> Self::Output {
        self.build(PadStartOperation { width, character })
    }
}

operation_manifest! {
    PadStartOperation<A> {
        method: PadStart<A>::pad_start;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueString>;
            field: width: usize;
            argument: A: ArgumentSource<Keyed<I>> where A::ValueDomain: ValueString;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueString + BareValueDomain>;
            field: width: usize;
            argument: A: ArgumentSource<Unaligned> where A::ValueDomain: ValueString;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
