pub trait Index {
    type Output;

    fn index(&self) -> Self::Output;
}

pub trait Select {
    type Output;

    fn select(&self) -> Self::Output;
}

pub trait Resolve {
    type Output;

    fn resolve(&self) -> Self::Output;
}

pub trait ParentIndex {
    type Output;

    fn parent_index(&self) -> Self::Output;
}

pub trait ChildIndex {
    type Output;

    fn child_index(&self) -> Self::Output;
}
