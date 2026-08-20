pub trait Absolute {
    type Output;

    fn abs(&self) -> Self::Output;
}

pub trait Ceil {
    type Output;

    fn ceil(&self) -> Self::Output;
}

pub trait Clip<L, U> {
    type Output;

    fn clip(&self, lower: L, upper: U) -> Self::Output;
}

pub trait CubeRoot {
    type Output;

    fn cbrt(&self) -> Self::Output;
}

pub trait Exponential {
    type Output;

    fn exp(&self) -> Self::Output;
}

pub trait Floor {
    type Output;

    fn floor(&self) -> Self::Output;
}

pub trait Logarithm {
    type Output;

    fn log(&self) -> Self::Output;
}

pub trait Negate {
    type Output;

    fn neg(&self) -> Self::Output;
}

pub trait Round {
    type Output;

    fn round(&self) -> Self::Output;
}

pub trait Sign {
    type Output;

    fn sign(&self) -> Self::Output;
}

pub trait SquareRoot {
    type Output;

    fn sqrt(&self) -> Self::Output;
}
