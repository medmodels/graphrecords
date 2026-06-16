pub trait Sort {
    type ReturnOperand;

    fn sort(&self) -> Self::ReturnOperand;
}

pub trait SortBy<A> {
    type ReturnOperand;

    fn sort_by(&self, key: A) -> Self::ReturnOperand;
}

pub trait Unordered {
    type ReturnOperand;

    fn unordered(&self) -> Self::ReturnOperand;
}
