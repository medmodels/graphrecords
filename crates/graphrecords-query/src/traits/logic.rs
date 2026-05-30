pub trait And {
    type OtherOperand;
    type ReturnOperand;

    fn and(&self, other: Self::OtherOperand) -> Self::ReturnOperand;
}

pub trait Or {
    type OtherOperand;
    type ReturnOperand;

    fn or(&self, other: Self::OtherOperand) -> Self::ReturnOperand;
}

pub trait Not {
    type ReturnOperand;

    fn not(&self) -> Self::ReturnOperand;
}
