pub trait First {
    type Output;

    fn first(&self) -> Self::Output;
}

pub trait Last {
    type Output;

    fn last(&self) -> Self::Output;
}

pub trait ReverseOrder {
    type Output;

    fn reverse_order(&self) -> Self::Output;
}

pub trait Shuffle {
    type Output;

    fn shuffle(&self) -> Self::Output;
}

pub trait Sort {
    type Output;

    fn sort(&self) -> Self::Output;
}

pub trait SortBy<K> {
    type Output;

    fn sort_by(&self, key: K) -> Self::Output;
}

pub trait Take {
    type Output;

    fn take(&self, elements: usize) -> Self::Output;
}

pub trait Unorder {
    type Output;

    fn unorder(&self) -> Self::Output;
}
