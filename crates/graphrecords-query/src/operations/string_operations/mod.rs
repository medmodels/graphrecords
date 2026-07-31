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
    Failure, IndexDomain, Position, QueryResult, ValueDomain,
    capabilities::{IntValue, StringValue},
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Preserving, Retention},
    error::{
        numeric::{IntegerOverflow, NegativeLength},
        string::InvalidPaddingCharacter,
    },
    operations::{ArgumentSource, Keyed, Unaligned},
    registry::OperationManifest,
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

fn string_map_indexed<'a, I, V, W>(
    label: &'static str,
    operation: impl Fn(&'static str, String) -> QueryResult<W::Value<'a>> + 'a,
) -> IndexedValuePipeline<'a, I, V, W, Preserving>
where
    I: IndexDomain,
    V: StringValue,
    W: ValueDomain,
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
    W: ValueDomain,
{
    Pipeline::new(move |outcome| match outcome {
        Err(failure) => Err(failure),
        Ok(value) => V::into_string(label, value).and_then(|value| operation(label, value)),
    })
}

fn string_rebuild_map_indexed<'a, I, V>(
    label: &'static str,
    operation: impl Fn(&'static str, String) -> QueryResult<String> + 'a,
) -> IndexedValuePipeline<'a, I, V, V, Preserving>
where
    I: IndexDomain,
    V: StringValue,
{
    Pipeline::keyed(
        move |index, outcome: QueryResult<V::Value<'a>>| match outcome {
            Err(failure) => Err(failure),
            Ok(value) => {
                let role = value.clone();
                V::into_string(label, value)
                    .and_then(|value| operation(label, value))
                    .map(|value| V::from_string(&role, value))
                    .map_err(|failure| failure.at::<I>(&index))
            }
        },
    )
}

fn string_rebuild_map_bare<'a, V>(
    label: &'static str,
    operation: impl Fn(&'static str, String) -> QueryResult<String> + 'a,
) -> BarePipeline<'a, V, V, Preserving>
where
    V: StringValue,
{
    Pipeline::new(move |outcome: QueryResult<V::Value<'a>>| match outcome {
        Err(failure) => Err(failure),
        Ok(value) => {
            let role = value.clone();
            V::into_string(label, value)
                .and_then(|value| operation(label, value))
                .map(|value| V::from_string(&role, value))
        }
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
    W: ValueDomain,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: StringValue,
{
    Pipeline::keyed(move |index, outcome| {
        let value = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => match V::into_string(label, value) {
                Ok(value) => value,
                Err(failure) => {
                    return A::Retention::keep(Err(failure.at::<I>(&index)));
                }
            },
        };

        let step = A::resolve(&prepared, &index, label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument = A::ValueDomain::into_string(label, argument)
                    .map_err(|failure| failure.at::<I>(&index))?;

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
    W: ValueDomain,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: StringValue,
{
    Pipeline::new(move |outcome| {
        let value = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => match V::into_string(label, value) {
                Ok(value) => value,
                Err(failure) => {
                    return A::Retention::keep(Err(failure));
                }
            },
        };

        let step = A::resolve(&prepared, &(), label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument = A::ValueDomain::into_string(label, argument)?;

                operation(label, value, argument)
            })
        })
    })
}

fn string_rebuild_argument_map_indexed<'a, I, V, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    operation: impl Fn(&'static str, String, String) -> QueryResult<String> + 'a,
) -> IndexedValuePipeline<'a, I, V, V, A::Retention>
where
    I: IndexDomain,
    V: StringValue,
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: StringValue,
{
    Pipeline::keyed(move |index, outcome: QueryResult<V::Value<'a>>| {
        let (role, value) = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => {
                let role = value.clone();
                match V::into_string(label, value) {
                    Ok(value) => (role, value),
                    Err(failure) => {
                        return A::Retention::keep(Err(failure.at::<I>(&index)));
                    }
                }
            }
        };

        let step = A::resolve(&prepared, &index, label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument = A::ValueDomain::into_string(label, argument)
                    .map_err(|failure| failure.at::<I>(&index))?;

                operation(label, value, argument)
                    .map(|value| V::from_string(&role, value))
                    .map_err(|failure| failure.at::<I>(&index))
            })
        })
    })
}

fn string_rebuild_argument_map_bare<'a, V, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    operation: impl Fn(&'static str, String, String) -> QueryResult<String> + 'a,
) -> BarePipeline<'a, V, V, A::Retention>
where
    V: StringValue,
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: StringValue,
{
    Pipeline::new(move |outcome: QueryResult<V::Value<'a>>| {
        let (role, value) = match outcome {
            Err(failure) => return A::Retention::keep(Err(failure)),
            Ok(value) => {
                let role = value.clone();
                match V::into_string(label, value) {
                    Ok(value) => (role, value),
                    Err(failure) => return A::Retention::keep(Err(failure)),
                }
            }
        };

        let step = A::resolve(&prepared, &(), label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                let argument = A::ValueDomain::into_string(label, argument)?;

                operation(label, value, argument).map(|value| V::from_string(&role, value))
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
    A: ArgumentSource<Keyed<I>>,
    A::ValueDomain: StringValue,
    B: ArgumentSource<Keyed<I>>,
    B::ValueDomain: StringValue,
{
    Pipeline::keyed(move |index, outcome: QueryResult<V::Value<'a>>| {
        let (role, value) = match outcome {
            Err(failure) => {
                return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => {
                let role = value.clone();
                match V::into_string(label, value) {
                    Ok(value) => (role, value),
                    Err(failure) => {
                        return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(
                            Err(failure.at::<I>(&index)),
                        );
                    }
                }
            }
        };

        let first = A::resolve(&prepared.0, &index, label);

        A::Retention::and_then(first, |first| {
            let first = match A::ValueDomain::into_string(label, first) {
                Ok(first) => first,
                Err(failure) => {
                    return B::Retention::keep(Err(failure.at::<I>(&index)));
                }
            };

            let second = B::resolve(&prepared.1, &index, label);

            B::Retention::map_step(second, |second| {
                second.and_then(|second| {
                    let second = B::ValueDomain::into_string(label, second)
                        .map_err(|failure| failure.at::<I>(&index))?;

                    Ok(V::from_string(&role, operation(&value, &first, &second)))
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
    A: ArgumentSource<Unaligned>,
    A::ValueDomain: StringValue,
    B: ArgumentSource<Unaligned>,
    B::ValueDomain: StringValue,
{
    Pipeline::new(move |outcome: QueryResult<V::Value<'a>>| {
        let (role, value) = match outcome {
            Err(failure) => {
                return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => {
                let role = value.clone();
                match V::into_string(label, value) {
                    Ok(value) => (role, value),
                    Err(failure) => {
                        return <<A::Retention as Retention>::Then<B::Retention> as Retention>::keep(
                            Err(failure),
                        );
                    }
                }
            }
        };

        let first = A::resolve(&prepared.0, &(), label);

        A::Retention::and_then(first, |first| {
            let first = match A::ValueDomain::into_string(label, first) {
                Ok(first) => first,
                Err(failure) => {
                    return B::Retention::keep(Err(failure));
                }
            };

            let second = B::resolve(&prepared.1, &(), label);

            B::Retention::map_step(second, |second| {
                second.and_then(|second| {
                    let second = B::ValueDomain::into_string(label, second)?;

                    Ok(V::from_string(&role, operation(&value, &first, &second)))
                })
            })
        })
    })
}

fn padding_character(label: &'static str, value: String) -> QueryResult<char> {
    let mut characters = value.chars();
    let Some(character) = characters.next() else {
        return Err(Failure::new(label, InvalidPaddingCharacter::new(value)));
    };

    if characters.next().is_some() {
        return Err(Failure::new(label, InvalidPaddingCharacter::new(value)));
    }

    Ok(character)
}

fn padding_width(label: &'static str, value: i64) -> QueryResult<Position> {
    if value < 0 {
        return Err(Failure::new(label, NegativeLength::new(value)));
    }

    Position::try_from(value).map_err(|_| Failure::new(label, IntegerOverflow::new(value)))
}

fn string_pad_indexed<'a, I, V, W, C>(
    prepared: (W::Prepared<'a>, C::Prepared<'a>),
    label: &'static str,
    operation: impl Fn(&'static str, String, Position, String) -> QueryResult<String> + 'a,
) -> IndexedValuePipeline<'a, I, V, V, <W::Retention as Retention>::Then<C::Retention>>
where
    I: IndexDomain,
    V: StringValue,
    W: ArgumentSource<Keyed<I>>,
    W::ValueDomain: IntValue,
    C: ArgumentSource<Keyed<I>>,
    C::ValueDomain: StringValue,
{
    Pipeline::keyed(move |index, outcome: QueryResult<V::Value<'a>>| {
        let (role, value) = match outcome {
            Err(failure) => {
                return <<W::Retention as Retention>::Then<C::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => {
                let role = value.clone();
                match V::into_string(label, value) {
                    Ok(value) => (role, value),
                    Err(failure) => {
                        return <<W::Retention as Retention>::Then<C::Retention> as Retention>::keep(
                            Err(failure.at::<I>(&index)),
                        );
                    }
                }
            }
        };

        let width = W::resolve(&prepared.0, &index, label);

        W::Retention::and_then(width, |width| {
            let width = match W::ValueDomain::into_int(label, width)
                .and_then(|width| padding_width(label, width))
            {
                Ok(width) => width,
                Err(failure) => {
                    return C::Retention::keep(Err(failure.at::<I>(&index)));
                }
            };

            let character = C::resolve(&prepared.1, &index, label);

            C::Retention::map_step(character, |character| {
                character.and_then(|character| {
                    let character = C::ValueDomain::into_string(label, character)
                        .map_err(|failure| failure.at::<I>(&index))?;

                    operation(label, value, width, character)
                        .map(|value| V::from_string(&role, value))
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
    W: ArgumentSource<Unaligned>,
    W::ValueDomain: IntValue,
    C: ArgumentSource<Unaligned>,
    C::ValueDomain: StringValue,
{
    Pipeline::new(move |outcome: QueryResult<V::Value<'a>>| {
        let (role, value) = match outcome {
            Err(failure) => {
                return <<W::Retention as Retention>::Then<C::Retention> as Retention>::keep(Err(
                    failure,
                ));
            }
            Ok(value) => {
                let role = value.clone();
                match V::into_string(label, value) {
                    Ok(value) => (role, value),
                    Err(failure) => {
                        return <<W::Retention as Retention>::Then<C::Retention> as Retention>::keep(
                            Err(failure),
                        );
                    }
                }
            }
        };

        let width = W::resolve(&prepared.0, &(), label);

        W::Retention::and_then(width, |width| {
            let width = match W::ValueDomain::into_int(label, width)
                .and_then(|width| padding_width(label, width))
            {
                Ok(width) => width,
                Err(failure) => {
                    return C::Retention::keep(Err(failure));
                }
            };

            let character = C::resolve(&prepared.1, &(), label);

            C::Retention::map_step(character, |character| {
                character.and_then(|character| {
                    let character = C::ValueDomain::into_string(label, character)?;

                    operation(label, value, width, character)
                        .map(|value| V::from_string(&role, value))
                })
            })
        })
    })
}
