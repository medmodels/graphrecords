pub trait All {
    type Output;

    fn all(&self) -> Self::Output;
}

pub trait Any {
    type Output;

    fn any(&self) -> Self::Output;
}

pub trait Count {
    type Output;

    fn count(&self) -> Self::Output;
}

pub trait Maximum {
    type Output;

    fn max(&self) -> Self::Output;
}

pub trait Mean {
    type Output;

    fn mean(&self) -> Self::Output;
}

pub trait Median {
    type Output;

    fn median(&self) -> Self::Output;
}

pub trait Minimum {
    type Output;

    fn min(&self) -> Self::Output;
}

pub trait Mode {
    type Output;

    fn mode(&self) -> Self::Output;
}

pub trait UniqueCount {
    type Output;

    fn n_unique(&self) -> Self::Output;
}

pub trait Product {
    type Output;

    fn product(&self) -> Self::Output;
}

pub trait Random {
    type Output;

    fn random(&self) -> Self::Output;
}

pub trait StandardDeviation {
    type Output;

    fn std(&self) -> Self::Output;
}

pub trait Sum {
    type Output;

    fn sum(&self) -> Self::Output;
}

pub trait Variance {
    type Output;

    fn var(&self) -> Self::Output;
}
