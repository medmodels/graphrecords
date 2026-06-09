pub trait IsFloat {
    type ReturnOperand;

    fn is_float(&mut self) -> Self::ReturnOperand;
}

pub trait IsBool {
    type ReturnOperand;

    fn is_bool(&mut self) -> Self::ReturnOperand;
}

pub trait IsDateTime {
    type ReturnOperand;

    fn is_datetime(&mut self) -> Self::ReturnOperand;
}

pub trait IsDuration {
    type ReturnOperand;

    fn is_duration(&mut self) -> Self::ReturnOperand;
}

pub trait IsNull {
    type ReturnOperand;

    fn is_null(&mut self) -> Self::ReturnOperand;
}

pub trait IsString {
    type ReturnOperand;

    fn is_string(&mut self) -> Self::ReturnOperand;
}

pub trait IsInt {
    type ReturnOperand;

    fn is_int(&mut self) -> Self::ReturnOperand;
}
