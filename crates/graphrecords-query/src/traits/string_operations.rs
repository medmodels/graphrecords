pub trait Trim {
    type ReturnOperand;

    fn trim(&mut self) -> Self::ReturnOperand;
}

pub trait TrimStart {
    type ReturnOperand;

    fn trim_start(&mut self) -> Self::ReturnOperand;
}

pub trait TrimEnd {
    type ReturnOperand;

    fn trim_end(&mut self) -> Self::ReturnOperand;
}

pub trait Lowercase {
    type ReturnOperand;

    fn lowercase(&mut self) -> Self::ReturnOperand;
}

pub trait Uppercase {
    type ReturnOperand;

    fn uppercase(&mut self) -> Self::ReturnOperand;
}

pub trait Slice {
    type ReturnOperand;

    fn slice(&mut self, start: usize, end: usize) -> Self::ReturnOperand;
}

pub trait StartsWith {
    type ComparisonOperand;
    type ReturnOperand;

    fn starts_with<V: Into<Self::ComparisonOperand>>(&mut self, value: V) -> Self::ReturnOperand;
}

pub trait EndsWith {
    type ComparisonOperand;
    type ReturnOperand;

    fn ends_with<V: Into<Self::ComparisonOperand>>(&mut self, value: V) -> Self::ReturnOperand;
}

pub trait Contains {
    type ComparisonOperand;
    type ReturnOperand;

    fn contains<V: Into<Self::ComparisonOperand>>(&mut self, value: V) -> Self::ReturnOperand;
}
