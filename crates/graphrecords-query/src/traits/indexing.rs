pub trait Index {
    type ReturnOperand;

    fn index(&self) -> Self::ReturnOperand;
}

pub trait Select {
    type ReturnOperand;

    fn select(&self) -> Self::ReturnOperand;
}
