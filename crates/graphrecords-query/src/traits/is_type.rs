pub trait IsBool {
    type Output;

    fn is_bool(&self) -> Self::Output;
}

pub trait IsDateTime {
    type Output;

    fn is_datetime(&self) -> Self::Output;
}

pub trait IsDuration {
    type Output;

    fn is_duration(&self) -> Self::Output;
}

pub trait IsFloat {
    type Output;

    fn is_float(&self) -> Self::Output;
}

pub trait IsInt {
    type Output;

    fn is_int(&self) -> Self::Output;
}

pub trait IsNull {
    type Output;

    fn is_null(&self) -> Self::Output;
}

pub trait IsString {
    type Output;

    fn is_string(&self) -> Self::Output;
}
