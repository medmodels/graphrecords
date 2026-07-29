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
    Failure, IndexDomain, Position, QueryResult, ValueType,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Preserving, Retention},
    operations::{ArgumentSource, Keyed, Unaligned},
    value::{InvalidPaddingCharacter, StringValue},
};
pub use contains::ContainsOperation;
pub use ends_with::EndsWithOperation;
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

fn string_map_indexed<'a, I, V, W>(
    label: &'static str,
    operation: impl Fn(&'static str, String) -> QueryResult<W::Value<'a>> + 'a,
) -> IndexedValuePipeline<'a, I, V, W, Preserving>
where
    I: IndexDomain,
    V: StringValue,
    W: ValueType,
{
    Pipeline::keyed(move |index, outcome| match outcome {
        Err(failure) => Err(failure),
        Ok(value) => V::into_string(label, value)
            .and_then(|value| operation(label, value))
            .map_err(|failure| failure.at::<I>(&index)),
    })
}

fn string_map_bare<'a, V, W>(
    label: &'static str,
    operation: impl Fn(&'static str, String) -> QueryResult<W::Value<'a>> + 'a,
) -> BarePipeline<'a, V, W, Preserving>
where
    V: StringValue,
    W: ValueType,
{
    Pipeline::new(move |outcome| match outcome {
        Err(failure) => Err(failure),
        Ok(value) => V::into_string(label, value).and_then(|value| operation(label, value)),
    })
}

fn string_argument_map_indexed<'a, I, V, W, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    operation: impl Fn(&'static str, String, String) -> QueryResult<W::Value<'a>> + 'a,
) -> IndexedValuePipeline<'a, I, V, W, A::Retention>
where
    I: IndexDomain,
    V: StringValue,
    W: ValueType,
    A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    Pipeline::keyed(move |index, outcome| {
        let value = match outcome {
            Err(failure) => return <A::Retention as Retention>::keep(Err(failure)),
            Ok(value) => match V::into_string(label, value) {
                Ok(value) => value,
                Err(failure) => {
                    return <A::Retention as Retention>::keep(Err(failure.at::<I>(&index)));
                }
            },
        };

        let step = A::resolve(&prepared, &index, label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument =
                    V::into_string(label, argument).map_err(|failure| failure.at::<I>(&index))?;

                operation(label, value, argument).map_err(|failure| failure.at::<I>(&index))
            })
        })
    })
}

fn string_argument_map_bare<'a, V, W, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    operation: impl Fn(&'static str, String, String) -> QueryResult<W::Value<'a>> + 'a,
) -> BarePipeline<'a, V, W, A::Retention>
where
    V: StringValue,
    W: ValueType,
    A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    Pipeline::new(move |outcome| {
        let value = match outcome {
            Err(failure) => return <A::Retention as Retention>::keep(Err(failure)),
            Ok(value) => match V::into_string(label, value) {
                Ok(value) => value,
                Err(failure) => {
                    return <A::Retention as Retention>::keep(Err(failure));
                }
            },
        };

        let step = A::resolve(&prepared, &(), label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument = V::into_string(label, argument)?;

                operation(label, value, argument)
            })
        })
    })
}

fn string_replace_indexed<'a, I, V, A, B>(
    prepared: (A::Prepared<'a>, B::Prepared<'a>),
    label: &'static str,
    operation: impl Fn(&str, &str, &str) -> String + 'a,
) -> IndexedValuePipeline<'a, I, V, V, <A::Retention as Retention>::Then<B::Retention>>
where
    I: IndexDomain,
    V: StringValue,
    A: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
    B: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    Pipeline::keyed(move |index, outcome| {
        let value = match outcome {
            Err(failure) => {
                return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => match V::into_string(label, value) {
                Ok(value) => value,
                Err(failure) => {
                    return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(
                        Err(failure.at::<I>(&index)),
                    );
                }
            },
        };

        let first = A::resolve(&prepared.0, &index, label);

        <A::Retention as Retention>::and_then(first, |first| {
            let first = match V::into_string(label, first) {
                Ok(first) => first,
                Err(failure) => {
                    return <B::Retention as Retention>::keep(Err(failure.at::<I>(&index)));
                }
            };

            let second = B::resolve(&prepared.1, &index, label);

            <B::Retention as Retention>::map_step(second, |second| {
                second.and_then(|second| {
                    let second =
                        V::into_string(label, second).map_err(|failure| failure.at::<I>(&index))?;

                    Ok(V::from_string(operation(&value, &first, &second)))
                })
            })
        })
    })
}

fn string_replace_bare<'a, V, A, B>(
    prepared: (A::Prepared<'a>, B::Prepared<'a>),
    label: &'static str,
    operation: impl Fn(&str, &str, &str) -> String + 'a,
) -> BarePipeline<'a, V, V, <A::Retention as Retention>::Then<B::Retention>>
where
    V: StringValue,
    A: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
    B: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    Pipeline::new(move |outcome| {
        let value = match outcome {
            Err(failure) => {
                return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => match V::into_string(label, value) {
                Ok(value) => value,
                Err(failure) => {
                    return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(
                        Err(failure),
                    );
                }
            },
        };

        let first = A::resolve(&prepared.0, &(), label);

        <A::Retention as Retention>::and_then(first, |first| {
            let first = match V::into_string(label, first) {
                Ok(first) => first,
                Err(failure) => {
                    return <B::Retention as Retention>::keep(Err(failure));
                }
            };

            let second = B::resolve(&prepared.1, &(), label);

            <B::Retention as Retention>::map_step(second, |second| {
                second.and_then(|second| {
                    let second = V::into_string(label, second)?;

                    Ok(V::from_string(operation(&value, &first, &second)))
                })
            })
        })
    })
}

fn padding_character(label: &'static str, value: String) -> QueryResult<char> {
    let mut characters = value.chars();
    let Some(character) = characters.next() else {
        return Err(Failure::new(label, InvalidPaddingCharacter { value }));
    };

    if characters.next().is_some() {
        return Err(Failure::new(label, InvalidPaddingCharacter { value }));
    }

    Ok(character)
}

fn string_pad_indexed<'a, I, V, W, C>(
    prepared: (W::Prepared<'a>, C::Prepared<'a>),
    label: &'static str,
    operation: impl Fn(&'static str, String, Position, String) -> QueryResult<String> + 'a,
) -> IndexedValuePipeline<'a, I, V, V, <W::Retention as Retention>::Then<C::Retention>>
where
    I: IndexDomain,
    V: StringValue,
    W: ArgumentSource<Keyed<I>, Value<'a> = Position>,
    C: ArgumentSource<Keyed<I>, Value<'a> = V::Value<'a>>,
{
    Pipeline::keyed(move |index, outcome| {
        let value = match outcome {
            Err(failure) => {
                return <<W::Retention as Retention>::Then<C::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => match V::into_string(label, value) {
                Ok(value) => value,
                Err(failure) => {
                    return <<W::Retention as Retention>::Then<C::Retention> as Retention>::keep(
                        Err(failure.at::<I>(&index)),
                    );
                }
            },
        };

        let width = W::resolve(&prepared.0, &index, label);

        <W::Retention as Retention>::and_then(width, |width| {
            let character = C::resolve(&prepared.1, &index, label);

            <C::Retention as Retention>::map_step(character, |character| {
                character.and_then(|character| {
                    let character = V::into_string(label, character)
                        .map_err(|failure| failure.at::<I>(&index))?;

                    operation(label, value, width, character)
                        .map(V::from_string)
                        .map_err(|failure| failure.at::<I>(&index))
                })
            })
        })
    })
}

fn string_pad_bare<'a, V, W, C>(
    prepared: (W::Prepared<'a>, C::Prepared<'a>),
    label: &'static str,
    operation: impl Fn(&'static str, String, Position, String) -> QueryResult<String> + 'a,
) -> BarePipeline<'a, V, V, <W::Retention as Retention>::Then<C::Retention>>
where
    V: StringValue,
    W: ArgumentSource<Unaligned, Value<'a> = Position>,
    C: ArgumentSource<Unaligned, Value<'a> = V::Value<'a>>,
{
    Pipeline::new(move |outcome| {
        let value = match outcome {
            Err(failure) => {
                return <<W::Retention as Retention>::Then<C::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => match V::into_string(label, value) {
                Ok(value) => value,
                Err(failure) => {
                    return <<W::Retention as Retention>::Then<C::Retention> as Retention>::keep(
                        Err(failure),
                    );
                }
            },
        };

        let width = W::resolve(&prepared.0, &(), label);

        <W::Retention as Retention>::and_then(width, |width| {
            let character = C::resolve(&prepared.1, &(), label);

            <C::Retention as Retention>::map_step(character, |character| {
                character.and_then(|character| {
                    let character = V::into_string(label, character)?;

                    operation(label, value, width, character).map(V::from_string)
                })
            })
        })
    })
}
