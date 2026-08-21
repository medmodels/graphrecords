mod aggregation;
mod arithmetic;
mod comparison;
mod conversion;
mod equivalence;
mod grouping;
mod integer;
mod kind;
mod numeric;
mod sortable;
mod string;
mod transition;

pub use aggregation::{ValueMedian, ValueMode, ValueScalar};
pub use arithmetic::{
    ValueAdd, ValueDivide, ValueModulo, ValueMultiply, ValuePower, ValueSubtract,
};
pub use comparison::{ValueEquality, ValueOrdering};
pub use conversion::ValueCast;
pub use equivalence::ValueEquivalence;
use graphrecords_core::graphrecord::{Identifier, IdentifierView, Value, ValueView};
pub use grouping::ValueGrouping;
pub use integer::ValueInt;
pub use kind::{PayloadKind, ValueKindTest, ValueScalarKindTest};
pub use numeric::{
    ValueAbsolute, ValueCeil, ValueClip, ValueCubeRoot, ValueExponential, ValueFloor,
    ValueLogarithm, ValueNegate, ValueRound, ValueSign, ValueSquareRoot,
};
pub use sortable::{EnsureSortable, incomparable_pair, incomparable_with_first};
pub use string::ValueString;
pub use transition::ValueTransition;

pub(crate) fn value_into_view<'a>(value: Value) -> ValueView<'a> {
    match value {
        Value::String(value) => ValueView::String(value.into()),
        Value::Int(value) => ValueView::Int(value),
        Value::Float(value) => ValueView::Float(value),
        Value::Bool(value) => ValueView::Bool(value),
        Value::DateTime(value) => ValueView::DateTime(value),
        Value::Duration(value) => ValueView::Duration(value),
        Value::Null => ValueView::Null,
    }
}

pub(crate) fn identifier_into_view<'a>(identifier: Identifier) -> IdentifierView<'a> {
    match identifier {
        Identifier::Int(value) => IdentifierView::Int(value),
        Identifier::String(value) => IdentifierView::String(value.into()),
    }
}
