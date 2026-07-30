pub trait IsBool {
    type ReturnOperand;

    fn is_bool(&self) -> Self::ReturnOperand;
}

pub trait IsDateTime {
    type ReturnOperand;

    fn is_datetime(&self) -> Self::ReturnOperand;
}

pub trait IsDuration {
    type ReturnOperand;

    fn is_duration(&self) -> Self::ReturnOperand;
}

pub trait IsFloat {
    type ReturnOperand;

    fn is_float(&self) -> Self::ReturnOperand;
}

pub trait IsInt {
    type ReturnOperand;

    fn is_int(&self) -> Self::ReturnOperand;
}

pub trait IsNull {
    type ReturnOperand;

    fn is_null(&self) -> Self::ReturnOperand;
}

pub trait IsString {
    type ReturnOperand;

    fn is_string(&self) -> Self::ReturnOperand;
}
