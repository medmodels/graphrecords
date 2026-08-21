mod contains;
mod ends_with;
mod length;
mod lowercase;
mod matches;
mod pad_end;
mod pad_start;
mod replace;
mod replace_all;
mod reverse;
mod slice;
mod split;
mod starts_with;
mod strip_prefix;
mod strip_suffix;
mod trim;
mod trim_end;
mod trim_start;
mod uppercase;

use crate::{
    Failure, IndexDomain, QueryResult, ValueDomain,
    capabilities::ValueString,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Preserving, Retention},
    error::string::InvalidPaddingCharacter,
    operations::{ArgumentSource, Keyed, Unaligned},
    registry::OperationManifest,
};
pub use contains::ContainsOperation;
pub use ends_with::EndsWithOperation;
use graphrecords_core::GraphRecord;
pub use length::LengthOperation;
pub use lowercase::LowercaseOperation;
pub use matches::MatchesOperation;
pub use pad_end::PadEndOperation;
pub use pad_start::PadStartOperation;
pub use replace::ReplaceOperation;
pub use replace_all::ReplaceAllOperation;
pub use reverse::ReverseOperation;
pub use slice::SliceOperation;
pub use split::SplitOperation;
pub use starts_with::StartsWithOperation;
pub use strip_prefix::StripPrefixOperation;
pub use strip_suffix::StripSuffixOperation;
pub use trim::TrimOperation;
pub use trim_end::TrimEndOperation;
pub use trim_start::TrimStartOperation;
pub use uppercase::UppercaseOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        contains::operation_manifest(),
        ends_with::operation_manifest(),
        length::operation_manifest(),
        lowercase::operation_manifest(),
        matches::operation_manifest(),
        pad_end::operation_manifest(),
        pad_start::operation_manifest(),
        replace::operation_manifest(),
        replace_all::operation_manifest(),
        reverse::operation_manifest(),
        slice::operation_manifest(),
        split::operation_manifest(),
        starts_with::operation_manifest(),
        strip_prefix::operation_manifest(),
        strip_suffix::operation_manifest(),
        trim::operation_manifest(),
        trim_end::operation_manifest(),
        trim_start::operation_manifest(),
        uppercase::operation_manifest(),
    ]
}

type StringMapFunction<'a, W> =
    fn(&str, &'static str) -> QueryResult<<W as ValueDomain>::Value<'a>>;

fn string_map_indexed<'a, I, V, W>(
    graphrecord: &'a GraphRecord,
    operation: StringMapFunction<'a, W>,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, W, Preserving>
where
    I: IndexDomain,
    V: ValueString,
    W: ValueDomain,
{
    Pipeline::keyed(move |address, outcome| match outcome {
        Err(failure) => Err(failure),
        Ok(value) => V::as_str(&value, label)
            .and_then(|value| operation(value, label))
            .map_err(|failure| failure.at_address::<I>(graphrecord, &address)),
    })
}

fn string_map_bare<'a, V, W>(
    operation: StringMapFunction<'a, W>,
    label: &'static str,
) -> BarePipeline<'a, V, W, Preserving>
where
    V: ValueString,
    W: ValueDomain,
{
    Pipeline::new(move |outcome| match outcome {
        Err(failure) => Err(failure),
        Ok(value) => V::as_str(&value, label).and_then(|value| operation(value, label)),
    })
}

fn string_rebuild_map_indexed<'a, I, V>(
    graphrecord: &'a GraphRecord,
    operation: impl Fn(&str, &'static str) -> QueryResult<String> + 'a,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, V, Preserving>
where
    I: IndexDomain,
    V: ValueString,
{
    Pipeline::keyed(
        move |address, outcome: QueryResult<V::Value<'a>>| match outcome {
            Err(failure) => Err(failure),
            Ok(value) => V::as_str(&value, label)
                .and_then(|string| operation(string, label))
                .map(|string| V::with_string(&value, string))
                .map_err(|failure| failure.at_address::<I>(graphrecord, &address)),
        },
    )
}

fn string_rebuild_map_bare<'a, V>(
    operation: impl Fn(&str, &'static str) -> QueryResult<String> + 'a,
    label: &'static str,
) -> BarePipeline<'a, V, V, Preserving>
where
    V: ValueString,
{
    Pipeline::new(move |outcome: QueryResult<V::Value<'a>>| match outcome {
        Err(failure) => Err(failure),
        Ok(value) => V::as_str(&value, label)
            .and_then(|string| operation(string, label))
            .map(|string| V::with_string(&value, string)),
    })
}

type StringArgumentMapFunction<'a, W> =
    fn(&str, &str, &'static str) -> QueryResult<<W as ValueDomain>::Value<'a>>;

fn string_argument_map_indexed<'a, I, V, W, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    operation: StringArgumentMapFunction<'a, W>,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, W, A::Retention>
where
    I: IndexDomain,
    V: ValueString,
    W: ValueDomain,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: ValueString,
{
    Pipeline::keyed(move |address, outcome| {
        let value = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => value,
        };

        let string = match V::as_str(&value, label) {
            Ok(string) => string,
            Err(failure) => {
                return A::Retention::keep(Err(failure.at_address::<I>(graphrecord, &address)));
            }
        };

        let step = A::resolve(graphrecord, &prepared, &address, label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument = A::ValueDomain::as_str(&argument, label)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;

                operation(string, argument, label)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))
            })
        })
    })
}

fn string_argument_map_bare<'a, V, W, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    operation: StringArgumentMapFunction<'a, W>,
    label: &'static str,
) -> BarePipeline<'a, V, W, A::Retention>
where
    V: ValueString,
    W: ValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: ValueString,
{
    Pipeline::new(move |outcome| {
        let value = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => value,
        };

        let string = match V::as_str(&value, label) {
            Ok(string) => string,
            Err(failure) => {
                return A::Retention::keep(Err(failure));
            }
        };

        let step = A::resolve(graphrecord, &prepared, &(), label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument = A::ValueDomain::as_str(&argument, label)?;

                operation(string, argument, label)
            })
        })
    })
}

type StringRebuildArgumentMapFunction = fn(&str, &str, &'static str) -> QueryResult<String>;

fn string_rebuild_argument_map_indexed<'a, I, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    operation: StringRebuildArgumentMapFunction,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, V, A::Retention>
where
    I: IndexDomain,
    V: ValueString,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: ValueString,
{
    Pipeline::keyed(move |address, outcome: QueryResult<V::Value<'a>>| {
        let value = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => value,
        };

        if let Err(failure) = V::as_str(&value, label) {
            return A::Retention::keep(Err(failure.at_address::<I>(graphrecord, &address)));
        }

        let step = A::resolve(graphrecord, &prepared, &address, label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument = A::ValueDomain::as_str(&argument, label)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;
                let string = V::as_str(&value, label)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;

                operation(string, argument, label)
                    .map(|string| V::with_string(&value, string))
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))
            })
        })
    })
}

fn string_rebuild_argument_map_bare<'a, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    operation: StringRebuildArgumentMapFunction,
    label: &'static str,
) -> BarePipeline<'a, V, V, A::Retention>
where
    V: ValueString,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: ValueString,
{
    Pipeline::new(move |outcome: QueryResult<V::Value<'a>>| {
        let value = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => value,
        };

        if let Err(failure) = V::as_str(&value, label) {
            return A::Retention::keep(Err(failure));
        }

        let step = A::resolve(graphrecord, &prepared, &(), label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument = A::ValueDomain::as_str(&argument, label)?;
                let string = V::as_str(&value, label)?;

                operation(string, argument, label).map(|string| V::with_string(&value, string))
            })
        })
    })
}

type StringReplaceFunction = fn(&str, &str, &str) -> String;

fn string_replace_indexed<'a, I, V, A, B>(
    graphrecord: &'a GraphRecord,
    prepared: (A::Prepared<'a>, B::Prepared<'a>),
    operation: StringReplaceFunction,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, V, <A::Retention as Retention>::Then<B::Retention>>
where
    I: IndexDomain,
    V: ValueString,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: ValueString,
    B: ArgumentSource<Keyed<I>>,
    B::ValueDomain: ValueString,
{
    Pipeline::keyed(move |address, outcome: QueryResult<V::Value<'a>>| {
        let value = match outcome {
            Err(failure) => {
                return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => value,
        };

        if let Err(failure) = V::as_str(&value, label) {
            return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(Err(
                failure.at_address::<I>(graphrecord, &address),
            ));
        }

        let first = A::resolve(graphrecord, &prepared.0, &address, label);

        A::Retention::and_then(first, |first| {
            let first = match A::ValueDomain::as_str(&first, label) {
                Ok(first) => first,
                Err(failure) => {
                    return B::Retention::keep(Err(failure.at_address::<I>(graphrecord, &address)));
                }
            };

            let second = B::resolve(graphrecord, &prepared.1, &address, label);

            B::Retention::map_step(second, |second| {
                second.and_then(|second| {
                    let second = B::ValueDomain::as_str(&second, label)
                        .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;
                    let value_string = V::as_str(&value, label)
                        .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;

                    Ok(V::with_string(
                        &value,
                        operation(value_string, first, second),
                    ))
                })
            })
        })
    })
}

fn string_replace_bare<'a, V, A, B>(
    graphrecord: &'a GraphRecord,
    prepared: (A::Prepared<'a>, B::Prepared<'a>),
    operation: StringReplaceFunction,
    label: &'static str,
) -> BarePipeline<'a, V, V, <A::Retention as Retention>::Then<B::Retention>>
where
    V: ValueString,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: ValueString,
    B: ArgumentSource<Unaligned>,
    B::ValueDomain: ValueString,
{
    Pipeline::new(move |outcome: QueryResult<V::Value<'a>>| {
        let value = match outcome {
            Err(failure) => {
                return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => value,
        };

        if let Err(failure) = V::as_str(&value, label) {
            return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(Err(
                failure,
            ));
        }

        let first = A::resolve(graphrecord, &prepared.0, &(), label);

        A::Retention::and_then(first, |first| {
            let first = match A::ValueDomain::as_str(&first, label) {
                Ok(first) => first,
                Err(failure) => {
                    return B::Retention::keep(Err(failure));
                }
            };

            let second = B::resolve(graphrecord, &prepared.1, &(), label);

            B::Retention::map_step(second, |second| {
                second.and_then(|second| {
                    let second = B::ValueDomain::as_str(&second, label)?;
                    let value_string = V::as_str(&value, label)?;

                    Ok(V::with_string(
                        &value,
                        operation(value_string, first, second),
                    ))
                })
            })
        })
    })
}

fn padding_character(value: &str, label: &'static str) -> QueryResult<char> {
    let mut characters = value.chars();
    let Some(character) = characters.next() else {
        return Err(Failure::new(
            InvalidPaddingCharacter::new(value.to_string()),
            label,
        ));
    };

    if characters.next().is_some() {
        return Err(Failure::new(
            InvalidPaddingCharacter::new(value.to_string()),
            label,
        ));
    }

    Ok(character)
}

type StringPadFunction = fn(&str, usize, &str, &'static str) -> QueryResult<String>;

fn string_pad_indexed<'a, I, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: (usize, A::Prepared<'a>),
    operation: StringPadFunction,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, V, A::Retention>
where
    I: IndexDomain,
    V: ValueString,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: ValueString,
{
    Pipeline::keyed(move |address, outcome: QueryResult<V::Value<'a>>| {
        let value = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => value,
        };

        if let Err(failure) = V::as_str(&value, label) {
            return A::Retention::keep(Err(failure.at_address::<I>(graphrecord, &address)));
        }

        let character = A::resolve(graphrecord, &prepared.1, &address, label);

        A::Retention::map_step(character, |character| {
            character.and_then(|character| {
                let character = A::ValueDomain::as_str(&character, label)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;
                let string = V::as_str(&value, label)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))?;

                operation(string, prepared.0, character, label)
                    .map(|string| V::with_string(&value, string))
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))
            })
        })
    })
}

fn string_pad_bare<'a, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: (usize, A::Prepared<'a>),
    operation: StringPadFunction,
    label: &'static str,
) -> BarePipeline<'a, V, V, A::Retention>
where
    V: ValueString,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: ValueString,
{
    Pipeline::new(move |outcome: QueryResult<V::Value<'a>>| {
        let value = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => value,
        };

        if let Err(failure) = V::as_str(&value, label) {
            return A::Retention::keep(Err(failure));
        }

        let character = A::resolve(graphrecord, &prepared.1, &(), label);

        A::Retention::map_step(character, |character| {
            character.and_then(|character| {
                let character = A::ValueDomain::as_str(&character, label)?;
                let string = V::as_str(&value, label)?;

                operation(string, prepared.0, character, label)
                    .map(|string| V::with_string(&value, string))
            })
        })
    })
}
