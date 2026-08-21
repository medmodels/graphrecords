pub trait IsIn<A> {
    type Output;

    fn is_in(&self, argument: A) -> Self::Output;
}
