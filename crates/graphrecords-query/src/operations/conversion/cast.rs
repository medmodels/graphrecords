use crate::{
    Bare, BareValueDomain, Explain, IndexDomain, Indexed, Labeled, QueryResult,
    capabilities::ValueCast,
    cast::{Bool, CastTarget, DateTime, Duration, Float, Int, String},
    element::{Pipeline, Preserving},
    execution::EvaluationCache,
    operations::{Build, ElementKernel, ElementPipeline, Operation, Prepare},
    optimizer::{Estimate, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs, Stats},
    registry::operation_manifest,
    traits::Cast,
};
use graphrecords_core::GraphRecord;

#[derive(Clone, Explain, Operation, OperationInputs, OptimizerHints, PlanIdentity, PlanInputs)]
#[operation(scope = Element)]
#[explain(label = "Cast")]
#[plan(optimizer_hints(allows_limit_pushdown, empty = if_any))]
pub struct CastOperation<T: CastTarget> {
    #[explain(label)]
    target: T,
}

impl<T: CastTarget> Prepare for CastOperation<T> {
    type Prepared<'a>
        = &'a T
    where
        Self: 'a;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(&self.target)
    }
}

impl<I: IndexDomain, V: ValueCast<T>, T: CastTarget> ElementKernel<Indexed<I, V>>
    for CastOperation<T>
{
    type Emission = Preserving;
    type OutShape = Indexed<I, V>;

    fn pipeline<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Indexed<I, V>, Self>> {
        Ok(Pipeline::keyed(move |address, outcome: QueryResult<_>| {
            outcome.and_then(|value| {
                V::cast(value, prepared, Self::LABEL)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))
            })
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<V: ValueCast<T> + BareValueDomain, T: CastTarget> ElementKernel<Bare<V>> for CastOperation<T> {
    type Emission = Preserving;
    type OutShape = Bare<V>;

    fn pipeline<'a>(
        _graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
    ) -> QueryResult<ElementPipeline<'a, Bare<V>, Self>> {
        Ok(Pipeline::new(move |outcome: QueryResult<_>| {
            outcome.and_then(|value| V::cast(value, prepared, Self::LABEL))
        }))
    }

    fn estimate(&self, input: Estimate, _stats: &Stats) -> Estimate {
        input.with_unknown_distinct()
    }
}

impl<E, T> Cast<T> for E
where
    CastOperation<T>: Operation,
    E: Build<CastOperation<T>>,
    T: CastTarget,
{
    type Output = E::Output;

    fn cast(&self, target: T) -> Self::Output {
        self.build(CastOperation { target })
    }
}

pub(super) mod bool {
    use super::{Bare, Bool, Cast, CastOperation, Indexed, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<Bool> {
            method: Cast<Bool>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<Bool>,>;
                selector: Bool;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<Bool> + BareValueDomain>;
                selector: Bool;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod date_time {
    use super::{Bare, Cast, CastOperation, DateTime, Indexed, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<DateTime> {
            method: Cast<DateTime>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<DateTime>,>;
                selector: DateTime;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<DateTime> + BareValueDomain>;
                selector: DateTime;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod duration {
    use super::{Bare, Cast, CastOperation, Duration, Indexed, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<Duration> {
            method: Cast<Duration>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<Duration>,>;
                selector: Duration;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<Duration> + BareValueDomain>;
                selector: Duration;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod float {
    use super::{Bare, Cast, CastOperation, Float, Indexed, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<Float> {
            method: Cast<Float>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<Float>,>;
                selector: Float;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<Float> + BareValueDomain>;
                selector: Float;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod int {
    use super::{Bare, Cast, CastOperation, Indexed, Int, Preserving, operation_manifest};

    operation_manifest! {
        CastOperation<Int> {
            method: Cast<Int>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<Int>,>;
                selector: Int;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<Int> + BareValueDomain>;
                selector: Int;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}

pub(super) mod string {
    use super::{Bare, Cast, CastOperation, Indexed, Preserving, String, operation_manifest};

    operation_manifest! {
        CastOperation<String> {
            method: Cast<String>::cast;
            scope: element;

            kernel {
                parameters: <I: IndexDomain, V: ValueCast<String>,>;
                selector: String;
                input: Indexed<I, V>;
                output: Indexed<I, V>;
                emission: Preserving;
            }

            kernel {
                parameters: <V: ValueCast<String> + BareValueDomain>;
                selector: String;
                input: Bare<V>;
                output: Bare<V>;
                emission: Preserving;
            }
        }
    }
}
