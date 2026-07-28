pub trait Add<A> {
    type ReturnOperand;

    fn add(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Subtract<A> {
    type ReturnOperand;

    fn subtract(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Multiply<A> {
    type ReturnOperand;

    fn multiply(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Divide<A> {
    type ReturnOperand;

    fn divide(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Power<A> {
    type ReturnOperand;

    fn power(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Modulo<A> {
    type ReturnOperand;

    fn modulo(&self, argument: A) -> Self::ReturnOperand;
}

pub trait Absolute {
    type ReturnOperand;

    fn absolute(&mut self) -> Self::ReturnOperand;
}
