pub trait DropDuplicates {
    type ReturnOperand;

    fn drop_duplicates(&self) -> Self::ReturnOperand;
}

pub trait IsDuplicated {
    type ReturnOperand;

    fn is_duplicated(&self) -> Self::ReturnOperand;
}

pub trait Unique {
    type ReturnOperand;

    fn unique(&self) -> Self::ReturnOperand;
}
