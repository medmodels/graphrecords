use crate::cast::CastTarget;

pub trait Cast<T: CastTarget> {
    type ReturnOperand;

    fn cast(&self, target: T) -> Self::ReturnOperand;
}

pub trait DiscardIndex {
    type ReturnOperand;

    fn discard_index(&self) -> Self::ReturnOperand;
}

pub trait DiscardValue {
    type ReturnOperand;

    fn discard_value(&self) -> Self::ReturnOperand;
}

pub trait Enumerate {
    type ReturnOperand;

    fn enumerate(&self) -> Self::ReturnOperand;
}

pub trait ExpandTo<T> {
    type ReturnOperand;

    fn expand_to(&self, template: &T) -> Self::ReturnOperand;
}
