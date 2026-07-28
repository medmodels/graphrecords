pub trait Discard {
    type ReturnOperand;

    fn discard(&self) -> Self::ReturnOperand;
}

pub trait ExpandTo<T> {
    type ReturnOperand;

    fn expand_to(&self, template: &T) -> Self::ReturnOperand;
}

pub trait ToValues {
    type ReturnOperand;

    fn to_values(&self) -> Self::ReturnOperand;
}

pub trait Enumerate {
    type ReturnOperand;

    fn enumerate(&self) -> Self::ReturnOperand;
}
