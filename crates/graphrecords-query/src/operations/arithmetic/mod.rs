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
use graphrecords_core::GraphRecord;
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
    <V as ValueDomain>::Value<'a>,
    <V as ValueDomain>::Value<'a>,
    &'static str,
) -> QueryResult<<V as ValueDomain>::Value<'a>>;

fn arithmetic_indexed<'a, I, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    operation: ArithmeticFunction<'a, V>,
    label: &'static str,
) -> IndexedValuePipeline<'a, I, V, V, A::Retention>
where
    I: IndexDomain,
    V: ValueDomain,
    A: ArgumentSource<Keyed<I>, V>,
    A::Prepared<'a>: 'a,
{
    Pipeline::keyed(move |address, item| {
        let value = match item {
            Ok(value) => value,
            Err(original) => {
                return A::Retention::keep(Err(original));
            }
        };

        let step = A::resolve(graphrecord, &prepared, &address, label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| {
                operation(value, argument, label)
                    .map_err(|failure| failure.at_address::<I>(graphrecord, &address))
            })
        })
    })
}

fn arithmetic_bare<'a, V, A>(
    graphrecord: &'a GraphRecord,
    prepared: A::Prepared<'a>,
    operation: ArithmeticFunction<'a, V>,
    label: &'static str,
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

        let step = A::resolve(graphrecord, &prepared, &(), label);

        A::Retention::map_step(step, |resolved| {
            resolved.and_then(|argument| operation(value, argument, label))
        })
    })
}
