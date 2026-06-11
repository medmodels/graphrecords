use crate::{
    IndexDomain,
    operations::{ArgumentSource, MissingPolicy, WithMissing},
};

pub trait MaybeAbsent<I: IndexDomain>: ArgumentSource<I> {
    fn on_missing<P>(self, policy: P) -> WithMissing<I, Self, P>
    where
        Self: Sized,
        P: MissingPolicy<I, Self::Value>,
    {
        WithMissing::new(self, policy)
    }
}
