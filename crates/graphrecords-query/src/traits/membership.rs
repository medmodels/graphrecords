pub trait IsIn<A> {
    type ReturnOperand;

    fn is_in(&self, argument: A) -> Self::ReturnOperand;
}
