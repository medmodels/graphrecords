use crate::{
    Bare, ExpandedChild, ExpandedIndex, Explain, Failure, IndexDomain, Indexed, Labeled, Operand,
    Ordered, Positional, QueryResult,
    capabilities::StringValue,
    element::{Expanding, Pipeline, Retention},
    error::string::EmptySplitDelimiter,
    execution::EvaluationCache,
    operations::{
        Apply, ArgumentSource, ElementKernel, ElementPipeline, Keyed, Operation, OperationContext,
        Prepare, Unaligned,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    traits::Split,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Split")]
#[plan(optimizer_hints(empty = if_all))]
pub struct SplitOperation<A> {
    #[argument]
    delimiter: A,
}

impl<A: Prepare> Prepare for SplitOperation<A> {
    type Prepared<'a>
        = A::Prepared<'a>
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        self.delimiter.prepare(graphrecord, cache)
    }
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for SplitOperation<A>
where
    I: IndexDomain,
    V: StringValue,
    for<'a> A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    type Emission = Expanding<Ordered>;
    type OutShape = Indexed<ExpandedIndex<I, Positional>, V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::keyed(move |index, value| {
            let value =
                V::into_string(Self::LABEL, value).map_err(|failure| failure.at::<I>(&index))?;
            let delimiter = match <A::Retention as Retention>::collapse(A::resolve(
                &prepared,
                &index,
                Self::LABEL,
            )) {
                None => return Ok(Vec::new()),
                Some(Err(failure)) => return Err(failure),
                Some(Ok(delimiter)) => V::into_string(Self::LABEL, delimiter)
                    .map_err(|failure| failure.at::<I>(&index))?,
            };

            if delimiter.is_empty() {
                return Err(Failure::new_at::<I, _>(
                    Self::LABEL,
                    EmptySplitDelimiter,
                    &index,
                ));
            }

            Ok(value
                .split(&delimiter)
                .enumerate()
                .map(|(position, fragment)| {
                    ExpandedChild::success(position, V::from_string(fragment.to_owned()))
                })
                .collect())
        }))
    }
}

impl<V, A> ElementKernel<Bare<V>> for SplitOperation<A>
where
    V: StringValue,
    for<'a> A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    type Emission = Expanding<Ordered>;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(move |outcome| {
            let value = match outcome {
                Err(failure) => return vec![Err(failure)],
                Ok(value) => match V::into_string(Self::LABEL, value) {
                    Ok(value) => value,
                    Err(failure) => return vec![Err(failure)],
                },
            };
            let delimiter = match <A::Retention as Retention>::collapse(A::resolve(
                &prepared,
                &(),
                Self::LABEL,
            )) {
                None => return Vec::new(),
                Some(Err(failure)) => return vec![Err(failure)],
                Some(Ok(delimiter)) => match V::into_string(Self::LABEL, delimiter) {
                    Ok(delimiter) => delimiter,
                    Err(failure) => return vec![Err(failure)],
                },
            };

            if delimiter.is_empty() {
                return vec![Err(Failure::new(Self::LABEL, EmptySplitDelimiter))];
            }

            value
                .split(&delimiter)
                .map(|fragment| Ok(V::from_string(fragment.to_owned())))
                .collect()
        }))
    }
}

impl<O, A> Split<A> for O
where
    SplitOperation<A>: Operation,
    O: Apply<SplitOperation<A>>,
{
    type ReturnOperand = O::Output;

    fn split(&self, delimiter: A) -> Self::ReturnOperand {
        Self::ReturnOperand::new(OperationContext::new(
            self.clone(),
            SplitOperation { delimiter },
        ))
    }
}
