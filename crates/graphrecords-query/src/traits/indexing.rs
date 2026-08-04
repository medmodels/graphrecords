pub trait Index {
    type ReturnOperand;

    fn index(&self) -> Self::ReturnOperand;
}

pub trait Select {
    type ReturnOperand;

    fn select(&self) -> Self::ReturnOperand;
}

pub trait Resolve {
    type ReturnOperand;

    fn resolve(&self) -> Self::ReturnOperand;
}

pub trait ParentIndex {
    type ReturnOperand;

    fn parent_index(&self) -> Self::ReturnOperand;
}

pub trait ChildIndex {
    type ReturnOperand;

    fn child_index(&self) -> Self::ReturnOperand;
}
