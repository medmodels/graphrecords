use crate::{
    IndexDomain,
    operations::{ArgumentSource, Keyed, MissingPolicy, WithMissing},
};

pub trait MaybeAbsent<I: IndexDomain>: ArgumentSource<Keyed<I>> {
    fn on_missing<P>(self, policy: P) -> WithMissing<I, Self, P>
    where
        Self: Sized,
        P: MissingPolicy<I, Self>,
    {
        WithMissing::new(self, policy)
    }
}
