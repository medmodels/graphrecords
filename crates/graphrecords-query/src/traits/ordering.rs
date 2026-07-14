pub trait First {
    type ReturnOperand;

    fn first(&self) -> Self::ReturnOperand;
}

pub trait Last {
    type ReturnOperand;

    fn last(&self) -> Self::ReturnOperand;
}

pub trait Sort {
    type ReturnOperand;

    fn sort(&self) -> Self::ReturnOperand;
}

pub trait SortBy<A> {
    type ReturnOperand;

    fn sort_by(&self, key: A) -> Self::ReturnOperand;
}

pub trait Unorder {
    type ReturnOperand;

    fn unorder(&self) -> Self::ReturnOperand;
}
