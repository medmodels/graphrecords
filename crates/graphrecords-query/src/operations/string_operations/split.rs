use crate::{
    Bare, BareValueDomain, ExpandedChild, ExpandedIndex, Explain, Failure, IndexDomain, Indexed,
    Labeled, Ordered, Positional, QueryResult,
    capabilities::ValueString,
    element::{Expanding, Pipeline, Retention},
    error::string::EmptySplitDelimiter,
    operations::{
        ArgumentSource, Build, ElementKernel, ElementPipeline, Keyed, Operation, Prepare, Unaligned,
    },
    optimizer::{OperationInputs, OptimizerHints, PlanIdentity, PlanInputs},
    registry::operation_manifest,
    traits::Split,
};
use graphrecords_core::GraphRecord;

#[derive(
    Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Prepare,
)]
#[operation(scope = Element)]
#[explain(label = "Split")]
#[plan(optimizer_hints(empty = if_all))]
pub struct SplitOperation<A> {
    #[argument]
    delimiter: A,
}

impl<I, V, A> ElementKernel<Indexed<I, V>> for SplitOperation<A>
where
    I: IndexDomain,
    V: ValueString,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: ValueString,
{
    type Emission = Expanding<Ordered>;
    type OutShape = Indexed<ExpandedIndex<I, Positional>, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::keyed(move |address, value: V::Value<'a>| {
            let string = V::as_str(&value, Self::LABEL)
                .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;
            let delimiter = match A::Retention::collapse(A::resolve(
                graphrecord,
                &prepared,
                &address,
                Self::LABEL,
            )) {
                None => return Ok(Vec::new()),
                Some(Err(failure)) => return Err(failure),
                Some(Ok(delimiter)) => A::ValueDomain::as_str(&delimiter, Self::LABEL)
                    .map(str::to_string)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?,
            };

            if delimiter.is_empty() {
                return Err(Failure::new_at_address::<I, _>(
                    EmptySplitDelimiter,
                    graphrecord,
                    &address,
                    Self::LABEL,
                ));
            }

            Ok(string
                .split(&delimiter)
                .enumerate()
                .map(|(position, fragment)| {
                    ExpandedChild::success(position, V::with_string(&value, fragment.to_owned()))
                })
                .collect())
        }))
    }
}

impl<V, A> ElementKernel<Bare<V>> for SplitOperation<A>
where
    V: ValueString + BareValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: ValueString,
{
    type Emission = Expanding<Ordered>;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(move |outcome: QueryResult<V::Value<'a>>| {
            let value = match outcome {
                Err(failure) => return vec![Err(failure)],
                Ok(value) => value,
            };
            let string = match V::as_str(&value, Self::LABEL) {
                Ok(string) => string,
                Err(failure) => return vec![Err(failure)],
            };
            let delimiter = match A::Retention::collapse(A::resolve(
                graphrecord,
                &prepared,
                &(),
                Self::LABEL,
            )) {
                None => return Vec::new(),
                Some(Err(failure)) => return vec![Err(failure)],
                Some(Ok(delimiter)) => {
                    match A::ValueDomain::as_str(&delimiter, Self::LABEL).map(str::to_string) {
                        Ok(delimiter) => delimiter,
                        Err(failure) => return vec![Err(failure)],
                    }
                }
            };

            if delimiter.is_empty() {
                return vec![Err(Failure::new(EmptySplitDelimiter, Self::LABEL))];
            }

            string
                .split(&delimiter)
                .map(|fragment| Ok(V::with_string(&value, fragment.to_owned())))
                .collect()
        }))
    }
}

impl<E, A> Split<A> for E
where
    SplitOperation<A>: Operation,
    E: Build<SplitOperation<A>>,
{
    type Output = E::Output;

    fn split(&self, delimiter: A) -> Self::Output {
        self.build(SplitOperation { delimiter })
    }
}

operation_manifest! {
    SplitOperation<A> {
        method: Split<A>::split;
        scope: element;

        kernel {
            parameters: <I: IndexDomain, V: ValueString>;
            argument: A: ArgumentSource<Keyed<I>> where A::ValueDomain: ValueString;
            input: Indexed<I, V>;
            output: Indexed<ExpandedIndex<I, Positional>, V>;
            emission: Expanding<Ordered>;
        }

        kernel {
            parameters: <V: ValueString + BareValueDomain>;
            argument: A: ArgumentSource<Unaligned> where A::ValueDomain: ValueString;
            input: Bare<V>;
            output: Bare<V>;
            emission: Expanding<Ordered>;
        }
    }
}
