pub trait Add<A> {
    type Output;

    fn add(&self, argument: A) -> Self::Output;
}

pub trait Subtract<A> {
    type Output;

    fn subtract(&self, argument: A) -> Self::Output;
}

pub trait Multiply<A> {
    type Output;

    fn multiply(&self, argument: A) -> Self::Output;
}

pub trait Divide<A> {
    type Output;

    fn divide(&self, argument: A) -> Self::Output;
}

pub trait Power<A> {
    type Output;

    fn power(&self, argument: A) -> Self::Output;
}

pub trait Modulo<A> {
    type Output;

    fn modulo(&self, argument: A) -> Self::Output;
}
