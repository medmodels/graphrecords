pub trait Add {
    type OtherOperand;
    type ReturnOperand;

    fn add<O: Into<Self::OtherOperand>>(&mut self, other: O) -> Self::ReturnOperand;
}

pub trait Subtract<A> {
    type ReturnOperand;

    fn subtract(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Multiply {
    type OtherOperand;
    type ReturnOperand;

    fn multiply<O: Into<Self::OtherOperand>>(&mut self, other: O) -> Self::ReturnOperand;
}

pub trait Divide<A> {
    type ReturnOperand;

    fn divide(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Power {
    type ExponentOperand;
    type ReturnOperand;

    fn power<O: Into<Self::ExponentOperand>>(&mut self, exponent: O) -> Self::ReturnOperand;
}

pub trait Modulo<A> {
    type ReturnOperand;

    fn modulo(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Absolute {
    type ReturnOperand;

    fn absolute(&mut self) -> Self::ReturnOperand;
}
