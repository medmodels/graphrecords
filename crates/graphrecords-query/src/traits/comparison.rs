pub trait GreaterThan<A> {
    type Output;

    fn greater_than(&self, argument: A) -> Self::Output;
}

pub trait GreaterThanOrEqualTo<A> {
    type Output;

    fn greater_than_or_equal_to(&self, argument: A) -> Self::Output;
}

pub trait LessThan<A> {
    type Output;

    fn less_than(&self, argument: A) -> Self::Output;
}

pub trait LessThanOrEqualTo<A> {
    type Output;

    fn less_than_or_equal_to(&self, argument: A) -> Self::Output;
}

pub trait EqualTo<A> {
    type Output;

    fn equal_to(&self, argument: A) -> Self::Output;
}

pub trait NotEqualTo<A> {
    type Output;

    fn not_equal_to(&self, argument: A) -> Self::Output;
}
