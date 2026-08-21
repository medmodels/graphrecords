use crate::IndexDomain;

pub trait GroupBy<K> {
    type Output;

    fn group_by(&self, key: K) -> Self::Output;
}

pub trait Having<P> {
    type Output;

    fn having(&self, predicate: P) -> Self::Output;
}

pub trait Broadcast {
    type Output;

    fn broadcast(&self) -> Self::Output;
}

pub trait BroadcastVia<I: IndexDomain, A> {
    type Output;

    fn broadcast_via(&self, via: A) -> Self::Output;
}

pub trait Keys {
    type Output;

    fn keys(&self) -> Self::Output;
}

pub trait Ungroup {
    type Output;

    fn ungroup(&self) -> Self::Output;
}

pub trait UngroupKeyed {
    type Output;

    fn ungroup_keyed(&self) -> Self::Output;
}
