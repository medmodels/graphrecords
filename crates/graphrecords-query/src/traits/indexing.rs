pub trait Index {
    type ReturnOperand;

    fn index(&self) -> Self::ReturnOperand;
}
