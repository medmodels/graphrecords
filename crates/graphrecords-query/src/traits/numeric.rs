pub trait Round {
    type ReturnOperand;

    fn round(&mut self) -> Self::ReturnOperand;
}

pub trait Ceil {
    type ReturnOperand;

    fn ceil(&mut self) -> Self::ReturnOperand;
}

pub trait Floor {
    type ReturnOperand;

    fn floor(&mut self) -> Self::ReturnOperand;
}

pub trait SquareRoot {
    type ReturnOperand;

    fn sqrt(&mut self) -> Self::ReturnOperand;
}
