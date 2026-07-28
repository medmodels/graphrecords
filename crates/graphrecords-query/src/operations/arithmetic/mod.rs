mod absolute;
mod add;
mod divide;
mod modulo;
mod multiply;
mod power;
mod subtract;

use crate::{
    IndexDomain, QueryResult, ValueType,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Retention},
    operations::{ArgumentSource, Keyed, Unaligned},
};
pub use add::AddOperation;
pub use divide::DivideOperation;
pub use modulo::ModuloOperation;
pub use multiply::MultiplyOperation;
pub use power::PowerOperation;
pub use subtract::SubtractOperation;

type ArithmeticFunction<V> = fn(
    &'static str,
    <V as ValueType>::Owned,
    <V as ValueType>::Owned,
) -> QueryResult<<V as ValueType>::Owned>;

fn arithmetic_indexed<'a, I, A, V>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    operation: ArithmeticFunction<V>,
) -> IndexedValuePipeline<'a, I, V, V, A::Retention>
where
    I: IndexDomain,
    A: ArgumentSource<Keyed<I>, Value<'a> = <V as ValueType>::Owned>,
    A::Prepared<'a>: 'a,
    V: ValueType<Value<'a> = <V as ValueType>::Owned>,
{
    Pipeline::keyed(move |index, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &index, label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                operation(label, value, argument).map_err(|failure| failure.at::<I>(&index))
            })
        })
    })
}

fn arithmetic_bare<'a, A, V>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    operation: ArithmeticFunction<V>,
) -> BarePipeline<'a, V, V, A::Retention>
where
    A: ArgumentSource<Unaligned, Value<'a> = <V as ValueType>::Owned>,
    A::Prepared<'a>: 'a,
    V: ValueType<Value<'a> = <V as ValueType>::Owned>,
{
    Pipeline::new(move |item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return <A::Retention as Retention>::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &(), label);

        <A::Retention as Retention>::map_step(step, |resolved| {
            resolved.and_then(|argument| operation(label, value, argument))
        })
    })
}
