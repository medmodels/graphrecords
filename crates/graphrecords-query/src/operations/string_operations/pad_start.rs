use super::{padding_character, string_pad_bare, string_pad_indexed};
use crate::{
    Bare, Explain, External, Failure, IndexDomain, Indexed, Labeled, Operand, Position,
    QueryResult,
    capabilities::StringValue,
    element::Retention,
    error::string::StringPaddingOverflow,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    traits::PadStart,
};
use graphrecords_core::GraphRecord;

fn pad(
    label: &'static str,
    value: String,
    width: Position,
    character: String,
) -> QueryResult<String> {
    let character = padding_character(label, character)?;
    let padding_length = width.saturating_sub(value.chars().count());
    if padding_length == 0 {
        return Ok(value);
    }

    let capacity = padding_length
        .checked_mul(character.len_utf8())
        .and_then(|padding_bytes| padding_bytes.checked_add(value.len()))
        .ok_or_else(|| Failure::new(label, StringPaddingOverflow::new(width)))?;
    let mut padded = String::new();
    padded
        .try_reserve(capacity)
        .map_err(|error| Failure::new(label, External::new(error)))?;
    padded.extend(std::iter::repeat_n(character, padding_length));
    padded.push_str(&value);

    Ok(padded)
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "PadStart")]
#[plan(optimizer_hints(empty = if_all))]
pub struct PadStartOperation<W, C> {
    #[argument]
    width: W,
    #[argument]
    character: C,
}

impl<W: Prepare, C: Prepare> Prepare for PadStartOperation<W, C> {
    type Prepared<'a>
        = (W::Prepared<'a>, C::Prepared<'a>)
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok((
            self.width.prepare(graphrecord, cache)?,
            self.character.prepare(graphrecord, cache)?,
        ))
    }
}

impl<I, V, W, C> ElementKernel<Indexed<I, V>> for PadStartOperation<W, C>
where
    I: IndexDomain,
    V: StringValue,
    for<'a> W: ArgumentSource<Keyed<I>, Value<'a> = Position>,
    for<'a> C: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type Emission = <W::Retention as Retention>::Then<C::Retention>;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_pad_indexed::<_, V, W, C>(prepared, Self::LABEL, pad))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V, W, C> ElementKernel<Bare<V>> for PadStartOperation<W, C>
where
    V: StringValue,
    for<'a> W: ArgumentSource<Unaligned, Value<'a> = Position>,
    for<'a> C: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    type Emission = <W::Retention as Retention>::Then<C::Retention>;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_pad_bare::<V, W, C>(prepared, Self::LABEL, pad))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<O, W, C> PadStart<W, C> for O
where
    PadStartOperation<W, C>: Operation,
    O: Apply<PadStartOperation<W, C>>,
{
    type ReturnOperand = O::Output;

    fn pad_start(&self, width: W, character: C) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            PadStartOperation { width, character },
        ))
    }
}
