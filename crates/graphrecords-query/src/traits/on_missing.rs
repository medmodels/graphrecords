use crate::operations::{Alignment, ArgumentSource, MissingPolicy, WithMissing};

pub trait MaybeAbsent<A: Alignment>: ArgumentSource<A> {
    fn on_missing<P>(self, policy: P) -> WithMissing<A, Self, P>
    where
        Self: Sized,
        P: MissingPolicy<A, Self>,
    {
        WithMissing::new(self, policy)
    }
}
