pub trait And<O> {
    type ReturnOperand;

    fn and(&self, other: O) -> Self::ReturnOperand;
}

pub trait Or<O> {
    type ReturnOperand;

    fn or(&self, other: O) -> Self::ReturnOperand;
}

pub trait Xor<O> {
    type ReturnOperand;

    fn xor(&self, other: O) -> Self::ReturnOperand;
}

pub trait Not {
    type ReturnOperand;

    fn not(&self) -> Self::ReturnOperand;
}

pub trait IsMax {
    type ReturnOperand;

    fn is_max(&self) -> Self::ReturnOperand;
}

pub trait IsMin {
    type ReturnOperand;

    fn is_min(&self) -> Self::ReturnOperand;
}
