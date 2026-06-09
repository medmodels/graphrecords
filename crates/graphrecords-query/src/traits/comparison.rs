pub trait GreaterThan {
    type ComparisonOperand;
    type ReturnOperand;

    fn greater_than<V: Into<Self::ComparisonOperand>>(&mut self, value: V) -> Self::ReturnOperand;
}

pub trait GreaterThanOrEqualTo {
    type ComparisonOperand;
    type ReturnOperand;

    fn greater_than_or_equal_to<V: Into<Self::ComparisonOperand>>(
        &mut self,
        value: V,
    ) -> Self::ReturnOperand;
}

pub trait LessThan {
    type ComparisonOperand;
    type ReturnOperand;

    fn less_than<V: Into<Self::ComparisonOperand>>(&mut self, value: V) -> Self::ReturnOperand;
}

pub trait LessThanOrEqualTo {
    type ComparisonOperand;
    type ReturnOperand;

    fn less_than_or_equal_to<V: Into<Self::ComparisonOperand>>(
        &mut self,
        value: V,
    ) -> Self::ReturnOperand;
}

pub trait EqualTo {
    type ComparisonOperand;
    type ReturnOperand;

    fn equal_to<V: Into<Self::ComparisonOperand>>(&mut self, value: V) -> Self::ReturnOperand;
}

pub trait NotEqualTo {
    type ComparisonOperand;
    type ReturnOperand;

    fn not_equal_to<V: Into<Self::ComparisonOperand>>(&mut self, value: V) -> Self::ReturnOperand;
}
