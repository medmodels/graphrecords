pub trait Add {
    type OtherOperand;
    type ReturnOperand;

    fn add<O: Into<Self::OtherOperand>>(&mut self, other: O) -> Self::ReturnOperand;
}

pub trait Subtract {
    type OtherOperand;
    type ReturnOperand;

    fn subtract<O: Into<Self::OtherOperand>>(&mut self, other: O) -> Self::ReturnOperand;
}

pub trait Multiply {
    type OtherOperand;
    type ReturnOperand;

    fn multiply<O: Into<Self::OtherOperand>>(&mut self, other: O) -> Self::ReturnOperand;
}

pub trait Divide {
    type OtherOperand;
    type ReturnOperand;

    fn divide<O: Into<Self::OtherOperand>>(&mut self, other: O) -> Self::ReturnOperand;
}

pub trait Power {
    type ExponentOperand;
    type ReturnOperand;

    fn power<O: Into<Self::ExponentOperand>>(&mut self, exponent: O) -> Self::ReturnOperand;
}

pub trait Modulo<Argument> {
    type Output;

    fn modulo(&self, argument: Argument) -> Self::Output;
}

pub trait Absolute {
    type ReturnOperand;

    fn absolute(&mut self) -> Self::ReturnOperand;
}
