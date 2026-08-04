pub trait GreaterThan<A> {
    type ReturnOperand;

    fn greater_than(&self, argument: A) -> Self::ReturnOperand;
}

pub trait GreaterThanOrEqualTo<A> {
    type ReturnOperand;

    fn greater_than_or_equal_to(&self, argument: A) -> Self::ReturnOperand;
}

pub trait LessThan<A> {
    type ReturnOperand;

    fn less_than(&self, argument: A) -> Self::ReturnOperand;
}

pub trait LessThanOrEqualTo<A> {
    type ReturnOperand;

    fn less_than_or_equal_to(&self, argument: A) -> Self::ReturnOperand;
}

pub trait EqualTo<A> {
    type ReturnOperand;

    fn equal_to(&self, argument: A) -> Self::ReturnOperand;
}

pub trait NotEqualTo<A> {
    type ReturnOperand;

    fn not_equal_to(&self, argument: A) -> Self::ReturnOperand;
}
