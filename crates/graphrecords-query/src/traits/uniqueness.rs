pub trait DropDuplicates {
    type Output;

    fn drop_duplicates(&self) -> Self::Output;
}

pub trait IsDuplicated {
    type Output;

    fn is_duplicated(&self) -> Self::Output;
}

pub trait Unique {
    type Output;

    fn unique(&self) -> Self::Output;
}
