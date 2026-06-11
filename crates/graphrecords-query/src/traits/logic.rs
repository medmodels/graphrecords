pub trait And<Other> {
    type ReturnOperand;

    fn and(&self, other: Other) -> Self::ReturnOperand;
}

pub trait Or<Other> {
    type ReturnOperand;

    fn or(&self, other: Other) -> Self::ReturnOperand;
}

pub trait Xor<Other> {
    type ReturnOperand;

    fn xor(&self, other: Other) -> Self::ReturnOperand;
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
