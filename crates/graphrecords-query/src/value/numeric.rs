use crate::{
    AttributeName, Diagnostic, Failure, IncomparableValues, IndexValue, Positional, QueryResult,
    Scalar, ValueType,
};
use graphrecords_core::graphrecord::{
    EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex,
    datatypes::{Abs, Ceil, Floor, Round, Sqrt},
};
use std::{
    cmp::Ordering,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

pub trait ValueAbsolute: ValueType {
    fn absolute(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueCeil: ValueType {
    fn ceil(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueClip: ValueType {
    fn clip(
        label: &'static str,
        value: Self::Owned,
        lower: Self::Owned,
        upper: Self::Owned,
    ) -> QueryResult<Self::Owned>;
}

pub trait ValueCubeRoot: ValueType {
    fn cube_root(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueExponential: ValueType {
    fn exponential(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueFloor: ValueType {
    fn floor(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueLogarithm: ValueType {
    fn logarithm(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueNegate: ValueType {
    fn negate(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueRound: ValueType {
    fn round(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueSign: ValueType {
    fn sign(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueSquareRoot: ValueType {
    fn square_root(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

#[derive(Debug)]
pub struct InvalidClipBounds<T> {
    pub lower: T,
    pub upper: T,
}

impl<T: Display> Display for InvalidClipBounds<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "lower clip bound `{}` exceeds upper clip bound `{}`",
            self.lower, self.upper
        )
    }
}

impl<T: Debug + Display> Error for InvalidClipBounds<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for InvalidClipBounds<T> {
    fn name() -> &'static str {
        "InvalidClipBounds"
    }

    fn help(&self) -> Option<String> {
        Some("provide a lower bound that does not exceed the upper bound".to_string())
    }
}

#[derive(Debug)]
pub struct NegativeSquareRoot {
    pub value: GraphRecordValue,
}

impl Display for NegativeSquareRoot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot take the square root of negative `{}`",
            self.value
        )
    }
}

impl Error for NegativeSquareRoot {}

impl Diagnostic for NegativeSquareRoot {
    fn name() -> &'static str {
        "NegativeSquareRoot"
    }
}

#[derive(Debug)]
pub struct NonNumericValue<T> {
    pub value: T,
}

impl<T: Display> Display for NonNumericValue<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "`{}` is not a numeric value", self.value)
    }
}

impl<T: Debug + Display> Error for NonNumericValue<T> {}

impl<T: Debug + Display + Send + Sync + 'static> Diagnostic for NonNumericValue<T> {
    fn name() -> &'static str {
        "NonNumericValue"
    }
}

#[derive(Debug)]
pub struct NonPositiveLogarithm {
    pub value: GraphRecordValue,
}

impl Display for NonPositiveLogarithm {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cannot take the logarithm of non-positive `{}`",
            self.value
        )
    }
}

impl Error for NonPositiveLogarithm {}

impl Diagnostic for NonPositiveLogarithm {
    fn name() -> &'static str {
        "NonPositiveLogarithm"
    }
}

fn clip_ordered<T>(label: &'static str, value: T, lower: T, upper: T) -> QueryResult<T>
where
    T: Debug + Display + PartialOrd + Send + Sync + 'static,
{
    match lower.partial_cmp(&upper) {
        Some(Ordering::Greater) => {
            return Err(Failure::new(label, InvalidClipBounds { lower, upper }));
        }
        None => {
            return Err(Failure::new(
                label,
                IncomparableValues {
                    first: lower,
                    second: upper,
                },
            ));
        }
        Some(Ordering::Less | Ordering::Equal) => {}
    }

    match value.partial_cmp(&lower) {
        Some(Ordering::Less) => return Ok(lower),
        None => {
            return Err(Failure::new(
                label,
                IncomparableValues {
                    first: value,
                    second: lower,
                },
            ));
        }
        Some(Ordering::Equal | Ordering::Greater) => {}
    }

    match value.partial_cmp(&upper) {
        Some(Ordering::Greater) => Ok(upper),
        Some(Ordering::Less | Ordering::Equal) => Ok(value),
        None => Err(Failure::new(
            label,
            IncomparableValues {
                first: value,
                second: upper,
            },
        )),
    }
}

fn clip_graphrecord_attribute(
    label: &'static str,
    value: GraphRecordAttribute,
    lower: GraphRecordAttribute,
    upper: GraphRecordAttribute,
) -> QueryResult<GraphRecordAttribute> {
    let value = match value {
        GraphRecordAttribute::Int(_) => value,
        value @ GraphRecordAttribute::String(_) => {
            return Err(Failure::new(label, NonNumericValue { value }));
        }
    };
    let lower = match lower {
        GraphRecordAttribute::Int(_) => lower,
        lower @ GraphRecordAttribute::String(_) => {
            return Err(Failure::new(label, NonNumericValue { value: lower }));
        }
    };
    let upper = match upper {
        GraphRecordAttribute::Int(_) => upper,
        upper @ GraphRecordAttribute::String(_) => {
            return Err(Failure::new(label, NonNumericValue { value: upper }));
        }
    };

    clip_ordered(label, value, lower, upper)
}

fn clip_graphrecord_value(
    label: &'static str,
    value: GraphRecordValue,
    lower: GraphRecordValue,
    upper: GraphRecordValue,
) -> QueryResult<GraphRecordValue> {
    let value = match value {
        GraphRecordValue::Int(_)
        | GraphRecordValue::Float(_)
        | GraphRecordValue::DateTime(_)
        | GraphRecordValue::Duration(_) => value,
        value => return Err(Failure::new(label, NonNumericValue { value })),
    };
    let lower = match lower {
        GraphRecordValue::Int(_)
        | GraphRecordValue::Float(_)
        | GraphRecordValue::DateTime(_)
        | GraphRecordValue::Duration(_) => lower,
        lower => return Err(Failure::new(label, NonNumericValue { value: lower })),
    };
    let upper = match upper {
        GraphRecordValue::Int(_)
        | GraphRecordValue::Float(_)
        | GraphRecordValue::DateTime(_)
        | GraphRecordValue::Duration(_) => upper,
        upper => return Err(Failure::new(label, NonNumericValue { value: upper })),
    };

    clip_ordered(label, value, lower, upper)
}

impl ValueAbsolute for Scalar {
    fn absolute(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.abs()),
            GraphRecordValue::Duration(duration) => Ok(GraphRecordValue::Duration(duration.abs())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueAbsolute for AttributeName {
    fn absolute(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordAttribute::Int(_) => Ok(value.abs()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueAbsolute for IndexValue<NodeIndex> {
    fn absolute(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordAttribute::Int(_) => Ok(value.abs()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueAbsolute for IndexValue<AttributeName> {
    fn absolute(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordAttribute::Int(_) => Ok(value.abs()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueAbsolute for IndexValue<GraphRecordValue> {
    fn absolute(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.abs()),
            GraphRecordValue::Duration(duration) => Ok(GraphRecordValue::Duration(duration.abs())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueCeil for Scalar {
    fn ceil(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.ceil()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueCeil for IndexValue<GraphRecordValue> {
    fn ceil(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.ceil()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueClip for Scalar {
    fn clip(
        label: &'static str,
        value: Self::Owned,
        lower: Self::Owned,
        upper: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        clip_graphrecord_value(label, value, lower, upper)
    }
}

impl ValueClip for AttributeName {
    fn clip(
        label: &'static str,
        value: Self::Owned,
        lower: Self::Owned,
        upper: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        clip_graphrecord_attribute(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<Positional> {
    fn clip(
        label: &'static str,
        value: Self::Owned,
        lower: Self::Owned,
        upper: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        clip_ordered(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<NodeIndex> {
    fn clip(
        label: &'static str,
        value: Self::Owned,
        lower: Self::Owned,
        upper: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        clip_graphrecord_attribute(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<AttributeName> {
    fn clip(
        label: &'static str,
        value: Self::Owned,
        lower: Self::Owned,
        upper: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        clip_graphrecord_attribute(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<EdgeIndex> {
    fn clip(
        label: &'static str,
        value: Self::Owned,
        lower: Self::Owned,
        upper: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        clip_ordered(label, value, lower, upper)
    }
}

impl ValueClip for IndexValue<GraphRecordValue> {
    fn clip(
        label: &'static str,
        value: Self::Owned,
        lower: Self::Owned,
        upper: Self::Owned,
    ) -> QueryResult<Self::Owned> {
        clip_graphrecord_value(label, value, lower, upper)
    }
}

impl ValueCubeRoot for Scalar {
    fn cube_root(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).cbrt())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.cbrt())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueCubeRoot for IndexValue<GraphRecordValue> {
    fn cube_root(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).cbrt())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.cbrt())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueExponential for Scalar {
    fn exponential(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).exp())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.exp())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueExponential for IndexValue<GraphRecordValue> {
    fn exponential(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).exp())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.exp())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueFloor for Scalar {
    fn floor(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.floor()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueFloor for IndexValue<GraphRecordValue> {
    fn floor(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.floor()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueLogarithm for Scalar {
    fn logarithm(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) if integer <= 0 => {
                Err(Failure::new(label, NonPositiveLogarithm { value }))
            }
            GraphRecordValue::Float(float) if float <= 0.0 => {
                Err(Failure::new(label, NonPositiveLogarithm { value }))
            }
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).ln())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.ln())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueLogarithm for IndexValue<GraphRecordValue> {
    fn logarithm(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) if integer <= 0 => {
                Err(Failure::new(label, NonPositiveLogarithm { value }))
            }
            GraphRecordValue::Float(float) if float <= 0.0 => {
                Err(Failure::new(label, NonPositiveLogarithm { value }))
            }
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Float((integer as f64).ln())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(float.ln())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueNegate for Scalar {
    fn negate(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Int(-integer)),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(-float)),
            GraphRecordValue::Duration(duration) => Ok(GraphRecordValue::Duration(-duration)),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueNegate for AttributeName {
    fn negate(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(-integer)),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueNegate for IndexValue<NodeIndex> {
    fn negate(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(-integer)),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueNegate for IndexValue<AttributeName> {
    fn negate(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(-integer)),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueNegate for IndexValue<GraphRecordValue> {
    fn negate(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Int(-integer)),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(-float)),
            GraphRecordValue::Duration(duration) => Ok(GraphRecordValue::Duration(-duration)),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueRound for Scalar {
    fn round(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.round()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueRound for IndexValue<GraphRecordValue> {
    fn round(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.round()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueSign for Scalar {
    fn sign(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Int(integer.signum())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(if float == 0.0 {
                0.0
            } else {
                float.signum()
            })),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueSign for AttributeName {
    fn sign(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(integer.signum())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueSign for IndexValue<NodeIndex> {
    fn sign(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(integer.signum())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueSign for IndexValue<AttributeName> {
    fn sign(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordAttribute::Int(integer) => Ok(GraphRecordAttribute::Int(integer.signum())),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueSign for IndexValue<GraphRecordValue> {
    fn sign(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) => Ok(GraphRecordValue::Int(integer.signum())),
            GraphRecordValue::Float(float) => Ok(GraphRecordValue::Float(if float == 0.0 {
                0.0
            } else {
                float.signum()
            })),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueSquareRoot for Scalar {
    fn square_root(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) if integer < 0 => {
                Err(Failure::new(label, NegativeSquareRoot { value }))
            }
            GraphRecordValue::Float(float) if float < 0.0 => {
                Err(Failure::new(label, NegativeSquareRoot { value }))
            }
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.sqrt()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}

impl ValueSquareRoot for IndexValue<GraphRecordValue> {
    fn square_root(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned> {
        match value {
            GraphRecordValue::Int(integer) if integer < 0 => {
                Err(Failure::new(label, NegativeSquareRoot { value }))
            }
            GraphRecordValue::Float(float) if float < 0.0 => {
                Err(Failure::new(label, NegativeSquareRoot { value }))
            }
            GraphRecordValue::Int(_) | GraphRecordValue::Float(_) => Ok(value.sqrt()),
            value => Err(Failure::new(label, NonNumericValue { value })),
        }
    }
}
