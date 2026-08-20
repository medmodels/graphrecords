use super::{padding_character, string_pad_bare, string_pad_indexed};
use crate::{
    Bare, BareValueDomain, Explain, External, Failure, IndexDomain, Indexed, Labeled, Position,
    QueryResult,
    capabilities::{ValueInt, ValueString},
    element::Retention,
    error::string::StringPaddingOverflow,
    execution::EvaluationCache,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::PadEnd,
};
use graphrecords_core::GraphRecord;
use std::iter::repeat_n;

fn pad(value: &str, width: Position, character: &str, label: &'static str) -> QueryResult<String> {
    let mut value = value.to_string();
    let character = padding_character(character, label)?;
    let padding_length = width.saturating_sub(value.chars().count());
    if padding_length == 0 {
        return Ok(value);
    }

    let capacity = padding_length
        .checked_mul(character.len_utf8())
        .and_then(|padding_bytes| padding_bytes.checked_add(value.len()))
        .ok_or_else(|| Failure::new(StringPaddingOverflow::new(width), label))?;
    value
        .try_reserve(capacity - value.len())
        .map_err(|error| Failure::new(External::new(error), label))?;
    value.extend(repeat_n(character, padding_length));

    Ok(value)
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "PadEnd")]
#[plan(optimizer_hints(empty = if_all))]
pub struct PadEndOperation<W, C> {
    #[argument]
    width: W,
    #[argument]
    character: C,
}

impl<W: Prepare, C: Prepare> Prepare for PadEndOperation<W, C> {
    type Prepared<'a>
        = (W::Prepared<'a>, C::Prepared<'a>)
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((
            self.width.prepare(graphrecord, cache)?,
            self.character.prepare(graphrecord, cache)?,
        ))
    }
}

impl<I, V, W, C> ElementKernel<Indexed<I, V>> for PadEndOperation<W, C>
where
    I: IndexDomain,
    V: ValueString,
    W: ArgumentSource<Keyed<I>>,
    W::ValueDomain: ValueInt,
    C: ArgumentSource<Keyed<I>>,
    C::ValueDomain: ValueString,
{
    type Emission = <W::Retention as Retention>::Then<C::Retention>;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_pad_indexed::<_, V, W, C>(
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

impl<V, W, C> ElementKernel<Bare<V>> for PadEndOperation<W, C>
where
    V: ValueString + BareValueDomain,
    W: ArgumentSource<Unaligned>,
    W::ValueDomain: ValueInt,
    C: ArgumentSource<Unaligned>,
    C::ValueDomain: ValueString,
{
    type Emission = <W::Retention as Retention>::Then<C::Retention>;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_pad_bare::<V, W, C>(
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

impl<E, W, C> PadEnd<W, C> for E
where
    PadEndOperation<W, C>: Operation,
    E: Build<PadEndOperation<W, C>>,
{
    type Output = E::Output;

    fn pad_end(&self, width: W, character: C) -> Self::Output {
        self.build(PadEndOperation { width, character })
    }
}

operation_manifest! {
    PadEndOperation<W, C> {
        method: PadEnd<W, C>::pad_end;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueString>;
            argument: W: ArgumentSource<Keyed<I>> where W::ValueDomain: ValueInt;
            argument: C: ArgumentSource<Keyed<I>> where C::ValueDomain: ValueString;
            input: Indexed<I, V>;
            output: Indexed<I, V>;
            emission: ArgumentRetention;
        }

        kernel {
            parameters: <V: ValueString + BareValueDomain>;
            argument: W: ArgumentSource<Unaligned> where W::ValueDomain: ValueInt;
            argument: C: ArgumentSource<Unaligned> where C::ValueDomain: ValueString;
            input: Bare<V>;
            output: Bare<V>;
            emission: ArgumentRetention;
        }
    }
}
