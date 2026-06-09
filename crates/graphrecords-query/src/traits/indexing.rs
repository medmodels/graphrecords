pub trait Index {
    type ReturnOperand;

    fn index(&mut self) -> Self::ReturnOperand;
}
