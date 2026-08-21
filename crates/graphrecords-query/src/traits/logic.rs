pub trait And<M> {
    type Output;

    fn and(&self, other: M) -> Self::Output;
}

pub trait Or<M> {
    type Output;

    fn or(&self, other: M) -> Self::Output;
}

pub trait ExclusiveOr<M> {
    type Output;

    fn xor(&self, other: M) -> Self::Output;
}

pub trait Not {
    type Output;

    fn not(&self) -> Self::Output;
}
