use crate::{AttributeName, Diagnostic, Failure, IndexValue, QueryResult, Scalar, ValueType};
use graphrecords_core::graphrecord::{
    GraphRecordAttribute, GraphRecordValue, NodeIndex,
    datatypes::{Abs, Ceil, Floor, Round, Sqrt},
};
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

pub trait ValueAbsolute: ValueType {
    fn absolute(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
}

pub trait ValueCeil: ValueType {
    fn ceil(label: &'static str, value: Self::Owned) -> QueryResult<Self::Owned>;
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
