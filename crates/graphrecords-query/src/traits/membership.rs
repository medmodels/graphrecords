pub trait IsIn {
    type ComparisonOperand;
    type ReturnOperand;

    fn is_in<V: Into<Self::ComparisonOperand>>(&mut self, values: V) -> Self::ReturnOperand;
}

pub trait IsNotIn {
    type ComparisonOperand;
    type ReturnOperand;

    fn is_not_in<V: Into<Self::ComparisonOperand>>(&mut self, values: V) -> Self::ReturnOperand;
}
