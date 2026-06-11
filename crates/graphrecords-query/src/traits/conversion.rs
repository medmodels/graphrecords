pub trait ToValues {
    type ReturnOperand;

    fn to_values(&self) -> Self::ReturnOperand;
}

pub trait Enumerate {
    type ReturnOperand;

    fn enumerate(&self) -> Self::ReturnOperand;
}
