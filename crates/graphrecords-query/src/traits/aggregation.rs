pub trait All {
    type ReturnOperand;

    fn all(&self) -> Self::ReturnOperand;
}

pub trait Any {
    type ReturnOperand;

    fn any(&self) -> Self::ReturnOperand;
}

pub trait Count {
    type ReturnOperand;

    fn count(&self) -> Self::ReturnOperand;
}

pub trait Maximum {
    type ReturnOperand;

    fn max(&self) -> Self::ReturnOperand;
}

pub trait Mean {
    type ReturnOperand;

    fn mean(&self) -> Self::ReturnOperand;
}

pub trait Median {
    type ReturnOperand;

    fn median(&mut self) -> Self::ReturnOperand;
}

pub trait Minimum {
    type ReturnOperand;

    fn min(&self) -> Self::ReturnOperand;
}

pub trait Mode {
    type ReturnOperand;

    fn mode(&self) -> Self::ReturnOperand;
}

pub trait UniqueCount {
    type ReturnOperand;

    fn n_unique(&self) -> Self::ReturnOperand;
}

pub trait Product {
    type ReturnOperand;

    fn product(&self) -> Self::ReturnOperand;
}

pub trait Random {
    type ReturnOperand;

    fn random(&self) -> Self::ReturnOperand;
}

pub trait StandardDeviation {
    type ReturnOperand;

    fn std(&self) -> Self::ReturnOperand;
}

pub trait Sum {
    type ReturnOperand;

    fn sum(&self) -> Self::ReturnOperand;
}

pub trait Variance {
    type ReturnOperand;

    fn var(&self) -> Self::ReturnOperand;
}
