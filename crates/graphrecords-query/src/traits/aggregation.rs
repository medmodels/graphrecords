pub trait Max {
    type ReturnOperand;

    fn max(&mut self) -> Self::ReturnOperand;
}

pub trait Min {
    type ReturnOperand;

    fn min(&mut self) -> Self::ReturnOperand;
}

pub trait Count {
    type ReturnOperand;

    fn count(&mut self) -> Self::ReturnOperand;
}

pub trait Sum {
    type ReturnOperand;

    fn sum(&mut self) -> Self::ReturnOperand;
}

pub trait Mean {
    type ReturnOperand;

    fn mean(&mut self) -> Self::ReturnOperand;
}

pub trait Median {
    type ReturnOperand;

    fn median(&mut self) -> Self::ReturnOperand;
}

pub trait Mode {
    type ReturnOperand;

    fn mode(&mut self) -> Self::ReturnOperand;
}

pub trait Std {
    type ReturnOperand;

    fn std(&mut self) -> Self::ReturnOperand;
}

pub trait Var {
    type ReturnOperand;

    fn var(&mut self) -> Self::ReturnOperand;
}
