use super::{string_argument_map_bare, string_argument_map_indexed};
use crate::{
    Bare, BareValueDomain, Explain, Failure, IndexDomain, Indexed, Labeled, Mask, Operand,
    QueryResult,
    capabilities::StringValue,
    error::string::InvalidRegexPattern,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::{describe::ArgumentRetention, operation_manifest},
    traits::Matches,
};
use graphrecords_core::GraphRecord;
use regex::Regex;

pub(super) fn regex_matches(label: &'static str, value: &str, pattern: &str) -> QueryResult<bool> {
    let expression = Regex::new(pattern).map_err(|error| {
        Failure::new(label, InvalidRegexPattern::new(pattern.to_string(), error))
    })?;

    Ok(expression.is_match(value))
}

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Matches")]
#[plan(optimizer_hints(empty = if_all))]
pub struct MatchesOperation<A> {
    #[argument]
    pattern: A,
}

impl<A: Prepare> Prepare for MatchesOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.pattern.prepare(graphrecord, cache)
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for MatchesOperation<A>
where
    I: IndexDomain,
    V: StringValue,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: StringValue,
{
    type Emission = A::Retention;
    type OutShape = Indexed<I, Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(string_argument_map_indexed::<_, V, Mask, A>(
            prepared,
            Self::LABEL,
            |label, value, pattern| regex_matches(label, &value, &pattern),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<V, A> ElementKernel<Bare<V>> for MatchesOperation<A>
where
    V: StringValue + BareValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: StringValue,
{
    type Emission = A::Retention;
    type OutShape = Bare<Mask>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(string_argument_map_bare::<V, Mask, A>(
            prepared,
            Self::LABEL,
            |label, value, pattern| regex_matches(label, &value, &pattern),
        ))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: None,
            ..input.with_unknown_distinct()
        }
    }
}

impl<O, A> Matches<A> for O
where
    MatchesOperation<A>: Operation,
    O: Apply<MatchesOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn matches(&self, pattern: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            MatchesOperation { pattern },
        ))
    }
}

operation_manifest! {
    MatchesOperation<A> {
        method: Matches<A>::matches;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: StringValue>;
            argument: A: ArgumentSource<Keyed<I>> where A::ValueDomain: StringValue;
            input: Indexed<I, V>;
            output: Indexed<I, Mask>;
            emission: ArgumentRetention;
        }
        kernel {
            parameters: <V: StringValue + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned> where A::ValueDomain: StringValue;
            input: Bare<V>;
            output: Bare<Mask>;
            emission: ArgumentRetention;
        }
    }
}
