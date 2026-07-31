mod add;
mod divide;
mod modulo;
mod multiply;
mod power;
mod subtract;

use crate::{
    IndexDomain, QueryResult, ValueDomain,
    element::{BarePipeline, IndexedValuePipeline, Pipeline, Retention},
    operations::{ArgumentSource, Keyed, Unaligned},
    registry::OperationManifest,
};
pub use add::AddOperation;
pub use divide::DivideOperation;
pub use modulo::ModuloOperation;
pub use multiply::MultiplyOperation;
pub use power::PowerOperation;
pub use subtract::SubtractOperation;

pub(super) fn operation_manifests() -> Vec<OperationManifest> {
    vec![
        add::operation_manifest(),
        divide::operation_manifest(),
        modulo::operation_manifest(),
        multiply::operation_manifest(),
        power::operation_manifest(),
        subtract::operation_manifest(),
    ]
}

type ArithmeticFunction<'a, V> = fn(
    &'static str,
    <V as ValueDomain>::Value<'a>,
    <V as ValueDomain>::Value<'a>,
) -> QueryResult<<V as ValueDomain>::Value<'a>>;

fn arithmetic_indexed<'a, I, V, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    operation: ArithmeticFunction<'a, V>,
) -> IndexedValuePipeline<'a, I, V, V, A::Retention>
where
    I: IndexDomain,
    V: ValueDomain,
    A: ArgumentSource<Keyed<I>, V>,
    A::Prepared<'a>: 'a,
{
    Pipeline::keyed(move |index, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return A::Retention::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &index, label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                operation(label, value, argument).map_err(|failure| failure.at::<I>(&index))
            })
        })
    })
}

fn arithmetic_bare<'a, V, A>(
    prepared: A::Prepared<'a>,
    label: &'static str,
    operation: ArithmeticFunction<'a, V>,
) -> BarePipeline<'a, V, V, A::Retention>
where
    V: ValueDomain,
    A: ArgumentSource<Unaligned, V>,
    A::Prepared<'a>: 'a,
{
    Pipeline::new(move |item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return A::Retention::keep(Err(original));
            }
        };

        let step = A::resolve(&prepared, &(), label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| operation(label, value, argument))
        })
    })
}
