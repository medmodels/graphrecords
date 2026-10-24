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

pub trait Sample {
    type Output;

    fn sample(&self, elements: usize) -> Self::Output;
}

pub trait SampleWithSeed {
    type Output;

    fn sample_with_seed(&self, elements: usize, seed: u64) -> Self::Output;
}

pub trait SampleFraction {
    type Output;

    fn sample_fraction(&self, fraction: f64) -> Self::Output;
}

pub trait SampleFractionWithSeed {
    type Output;

    fn sample_fraction_with_seed(&self, fraction: f64, seed: u64) -> Self::Output;
}

pub trait Shuffle {
    type Output;

    fn shuffle(&self) -> Self::Output;
}

pub trait ShuffleWithSeed {
    type Output;

    fn shuffle_with_seed(&self, seed: u64) -> Self::Output;
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

pub trait TakeFraction {
    type Output;

    fn take_fraction(&self, fraction: f64) -> Self::Output;
}

pub trait Unorder {
    type Output;

    fn unorder(&self) -> Self::Output;
}
