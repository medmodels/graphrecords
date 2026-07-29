pub trait Absolute {
    type ReturnOperand;

    fn abs(&self) -> Self::ReturnOperand;
}

pub trait Ceil {
    type ReturnOperand;

    fn ceil(&self) -> Self::ReturnOperand;
}

pub trait CubeRoot {
    type ReturnOperand;

    fn cbrt(&self) -> Self::ReturnOperand;
}

pub trait Exponential {
    type ReturnOperand;

    fn exp(&self) -> Self::ReturnOperand;
}

pub trait Floor {
    type ReturnOperand;

    fn floor(&self) -> Self::ReturnOperand;
}

pub trait Logarithm {
    type ReturnOperand;

    fn log(&self) -> Self::ReturnOperand;
}

pub trait Negate {
    type ReturnOperand;

    fn neg(&self) -> Self::ReturnOperand;
}

pub trait Round {
    type ReturnOperand;

    fn round(&self) -> Self::ReturnOperand;
}

pub trait Sign {
    type ReturnOperand;

    fn sign(&self) -> Self::ReturnOperand;
}

pub trait SquareRoot {
    type ReturnOperand;

    fn sqrt(&self) -> Self::ReturnOperand;
}
