use crate::{
    Arity, BoxedIterator, Definite, ElementShape, EvaluateExpression, Expression,
    ExpressionContext, Failure, IndexDomain, Indexed, Multiple, OrderState, QueryResult, Single,
    ValueDomain,
    error::{grouping::InvalidPartitionBucketArity, index::DuplicateIndex},
    execution::{CacheableExpression, EvaluationCache},
    explain::write_truncated_plan,
    expressions::ExpressionHandle,
    optimizer::{Estimate, Estimated, PlanNode, Stats},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::{fmt, marker::PhantomData, sync::Arc};

pub struct GroupedExpression<M: IndexDomain, K: IndexDomain, E: Expression> {
    context: Arc<dyn ExpressionContext<Self>>,
}

impl<M: IndexDomain, K: IndexDomain, E: Expression> Clone for GroupedExpression<M, K, E> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<M: IndexDomain, K: IndexDomain, E: Expression> fmt::Debug for GroupedExpression<M, K, E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Expression [")?;
        write_truncated_plan(formatter, self)?;
        formatter.write_str("]")
    }
}

impl<M: IndexDomain, K: IndexDomain, E: Expression> EvaluateExpression
    for GroupedExpression<M, K, E>
{
    type ReturnValue<'a>
        = Partition<'a, M, K, E>
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, cache)
    }
}

impl<M: IndexDomain, K: IndexDomain, E: Expression> Estimated for GroupedExpression<M, K, E> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.context().estimate(stats)
    }
}

impl<M: IndexDomain, K: IndexDomain, E: Expression> Expression for GroupedExpression<M, K, E> {
    fn context(&self) -> &dyn ExpressionContext<Self> {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<dyn ExpressionContext<Self>>) -> Self {
        Self { context }
    }
}

pub struct Partition<'a, M: IndexDomain, K: IndexDomain, E: Expression> {
    buckets: Vec<Bucket<'a, M, K, E>>,
    key_failures: Vec<KeyFailure<M>>,
}

pub struct Bucket<'a, M: IndexDomain, K: IndexDomain, E: Expression> {
    key: K::Owned,
    members: Vec<M::Address>,
    payload: QueryResult<E::ReturnValue<'a>>,
}

impl<'a, M: IndexDomain, K: IndexDomain, E: Expression> Bucket<'a, M, K, E> {
    #[must_use]
    pub const fn key(&self) -> &K::Owned {
        &self.key
    }

    #[must_use]
    pub fn members(&self) -> &[M::Address] {
        &self.members
    }

    pub const fn payload(&self) -> &QueryResult<E::ReturnValue<'a>> {
        &self.payload
    }
}

pub struct KeyFailure<M: IndexDomain> {
    member: M::Address,
    failure: Box<Failure>,
}

impl<M: IndexDomain> KeyFailure<M> {
    #[must_use]
    pub const fn member(&self) -> &M::Address {
        &self.member
    }

    #[must_use]
    pub fn failure(&self) -> &Failure {
        &self.failure
    }
}

pub type PartitionBucketParts<'a, M, K, E> = (
    <K as IndexDomain>::Owned,
    Vec<<M as IndexDomain>::Address>,
    QueryResult<<E as EvaluateExpression>::ReturnValue<'a>>,
);
pub type PartitionKeyFailureParts<M> = (<M as IndexDomain>::Address, Box<Failure>);
pub type PartitionParts<'a, M, K, E> = (
    Vec<PartitionBucketParts<'a, M, K, E>>,
    Vec<PartitionKeyFailureParts<M>>,
);

pub enum BucketChange<'a, E: Expression> {
    Drop,
    ReplacePayload(QueryResult<E::ReturnValue<'a>>),
}

pub enum KeyFailureChange {
    Drop,
    Raise,
}

impl<'a, M: IndexDomain, K: IndexDomain, E: Expression> Partition<'a, M, K, E> {
    #[must_use]
    pub fn buckets(&self) -> &[Bucket<'a, M, K, E>] {
        &self.buckets
    }

    #[must_use]
    pub fn key_failures(&self) -> &[KeyFailure<M>] {
        &self.key_failures
    }

    #[must_use]
    pub fn map_payloads<N: Expression>(
        self,
        mut function: impl FnMut(
            &K::Owned,
            &[M::Address],
            QueryResult<E::ReturnValue<'a>>,
        ) -> QueryResult<N::ReturnValue<'a>>,
    ) -> Partition<'a, M, K, N> {
        Partition {
            buckets: self
                .buckets
                .into_iter()
                .map(|bucket| Bucket {
                    payload: function(&bucket.key, &bucket.members, bucket.payload),
                    key: bucket.key,
                    members: bucket.members,
                })
                .collect(),
            key_failures: self.key_failures,
        }
    }

    #[cfg(feature = "dynamic")]
    pub(crate) fn map_domains<N: IndexDomain, L: IndexDomain>(
        self,
        mut map_member: impl FnMut(M::Address) -> N::Address,
        mut map_key: impl FnMut(K::Owned) -> L::Owned,
    ) -> Partition<'a, N, L, E> {
        Partition {
            buckets: self
                .buckets
                .into_iter()
                .map(|bucket| Bucket {
                    key: map_key(bucket.key),
                    members: bucket.members.into_iter().map(&mut map_member).collect(),
                    payload: bucket.payload,
                })
                .collect(),
            key_failures: self
                .key_failures
                .into_iter()
                .map(|key_failure| KeyFailure {
                    member: map_member(key_failure.member),
                    failure: key_failure.failure,
                })
                .collect(),
        }
    }

    #[must_use]
    pub fn change_buckets(
        self,
        mut function: impl FnMut(&Bucket<'a, M, K, E>) -> Option<BucketChange<'a, E>>,
    ) -> Self {
        let mut buckets = Vec::with_capacity(self.buckets.len());

        for mut bucket in self.buckets {
            let change = function(&bucket);

            match change {
                None => buckets.push(bucket),
                Some(BucketChange::Drop) => {}
                Some(BucketChange::ReplacePayload(payload)) => {
                    bucket.payload = payload;
                    buckets.push(bucket);
                }
            }
        }

        Self {
            buckets,
            key_failures: self.key_failures,
        }
    }

    pub fn change_key_failures(
        self,
        mut function: impl FnMut(&KeyFailure<M>) -> Option<KeyFailureChange>,
    ) -> QueryResult<Self> {
        let mut key_failures = Vec::with_capacity(self.key_failures.len());

        for key_failure in self.key_failures {
            let change = function(&key_failure);

            match change {
                None => key_failures.push(key_failure),
                Some(KeyFailureChange::Drop) => {}
                Some(KeyFailureChange::Raise) => return Err(key_failure.failure),
            }
        }

        Ok(Self {
            buckets: self.buckets,
            key_failures,
        })
    }

    #[must_use]
    pub fn into_parts(self) -> PartitionParts<'a, M, K, E> {
        (
            self.buckets
                .into_iter()
                .map(|bucket| (bucket.key, bucket.members, bucket.payload))
                .collect(),
            self.key_failures
                .into_iter()
                .map(|key_failure| (key_failure.member, key_failure.failure))
                .collect(),
        )
    }

    #[must_use]
    pub fn into_return_partition<T>(
        self,
        graphrecord: &'a GraphRecord,
        mut convert_payload: impl FnMut(QueryResult<E::ReturnValue<'a>>) -> QueryResult<T>,
    ) -> ReturnPartition<'a, M, K, T> {
        ReturnPartition {
            buckets: self
                .buckets
                .into_iter()
                .map(|bucket| ReturnBucket {
                    key: bucket.key,
                    members: bucket
                        .members
                        .iter()
                        .map(|member| M::index(graphrecord, member))
                        .collect(),
                    payload: convert_payload(bucket.payload),
                })
                .collect(),
            key_failures: self
                .key_failures
                .into_iter()
                .map(|key_failure| ReturnKeyFailure {
                    member: M::index(graphrecord, &key_failure.member),
                    failure: key_failure.failure,
                })
                .collect(),
        }
    }

    #[must_use]
    pub fn into_owned<T>(
        self,
        mut convert_payload: impl FnMut(QueryResult<E::ReturnValue<'a>>) -> QueryResult<T>,
    ) -> OwnedPartition<M, K, T> {
        OwnedPartition {
            buckets: self
                .buckets
                .into_iter()
                .map(|bucket| OwnedBucket {
                    key: bucket.key,
                    members: bucket.members,
                    payload: convert_payload(bucket.payload),
                })
                .collect(),
            key_failures: self
                .key_failures
                .into_iter()
                .map(|key_failure| OwnedKeyFailure {
                    member: key_failure.member,
                    failure: key_failure.failure,
                })
                .collect(),
        }
    }
}

pub struct ReturnPartition<'a, M: IndexDomain, K: IndexDomain, T> {
    buckets: Vec<ReturnBucket<'a, M, K, T>>,
    key_failures: Vec<ReturnKeyFailure<'a, M>>,
}

pub struct ReturnBucket<'a, M: IndexDomain, K: IndexDomain, T> {
    key: K::Owned,
    members: Vec<M::Index<'a>>,
    payload: QueryResult<T>,
}

impl<'a, M: IndexDomain, K: IndexDomain, T> ReturnBucket<'a, M, K, T> {
    #[must_use]
    pub const fn key(&self) -> &K::Owned {
        &self.key
    }

    #[must_use]
    pub fn members(&self) -> &[M::Index<'a>] {
        &self.members
    }

    pub const fn payload(&self) -> &QueryResult<T> {
        &self.payload
    }

    pub fn into_parts(self) -> (K::Owned, Vec<M::Index<'a>>, QueryResult<T>) {
        (self.key, self.members, self.payload)
    }
}

pub struct ReturnKeyFailure<'a, M: IndexDomain> {
    member: M::Index<'a>,
    failure: Box<Failure>,
}

impl<'a, M: IndexDomain> ReturnKeyFailure<'a, M> {
    #[must_use]
    pub const fn member(&self) -> &M::Index<'a> {
        &self.member
    }

    #[must_use]
    pub fn failure(&self) -> &Failure {
        &self.failure
    }

    #[must_use]
    pub fn into_parts(self) -> (M::Index<'a>, Box<Failure>) {
        (self.member, self.failure)
    }
}

pub type ReturnPartitionParts<'a, M, K, T> =
    (Vec<ReturnBucket<'a, M, K, T>>, Vec<ReturnKeyFailure<'a, M>>);

impl<'a, M: IndexDomain, K: IndexDomain, T> ReturnPartition<'a, M, K, T> {
    #[must_use]
    pub fn buckets(&self) -> &[ReturnBucket<'a, M, K, T>] {
        &self.buckets
    }

    #[must_use]
    pub fn key_failures(&self) -> &[ReturnKeyFailure<'a, M>] {
        &self.key_failures
    }

    #[must_use]
    pub fn into_parts(self) -> ReturnPartitionParts<'a, M, K, T> {
        (self.buckets, self.key_failures)
    }
}

pub struct OwnedPartition<M: IndexDomain, K: IndexDomain, T> {
    buckets: Vec<OwnedBucket<M, K, T>>,
    key_failures: Vec<OwnedKeyFailure<M>>,
}

pub struct OwnedBucket<M: IndexDomain, K: IndexDomain, T> {
    key: K::Owned,
    members: Vec<M::Address>,
    payload: QueryResult<T>,
}

impl<M: IndexDomain, K: IndexDomain, T> OwnedBucket<M, K, T> {
    #[must_use]
    pub const fn key(&self) -> &K::Owned {
        &self.key
    }

    #[must_use]
    pub fn members(&self) -> &[M::Address] {
        &self.members
    }

    pub const fn payload(&self) -> &QueryResult<T> {
        &self.payload
    }

    pub fn into_parts(self) -> (K::Owned, Vec<M::Address>, QueryResult<T>) {
        (self.key, self.members, self.payload)
    }
}

pub struct OwnedKeyFailure<M: IndexDomain> {
    member: M::Address,
    failure: Box<Failure>,
}

impl<M: IndexDomain> OwnedKeyFailure<M> {
    #[must_use]
    pub const fn member(&self) -> &M::Address {
        &self.member
    }

    #[must_use]
    pub fn failure(&self) -> &Failure {
        &self.failure
    }

    #[must_use]
    pub fn into_parts(self) -> (M::Address, Box<Failure>) {
        (self.member, self.failure)
    }
}

pub type OwnedPartitionParts<M, K, T> = (Vec<OwnedBucket<M, K, T>>, Vec<OwnedKeyFailure<M>>);

impl<M: IndexDomain, K: IndexDomain, T> OwnedPartition<M, K, T> {
    #[must_use]
    pub fn buckets(&self) -> &[OwnedBucket<M, K, T>] {
        &self.buckets
    }

    #[must_use]
    pub fn key_failures(&self) -> &[OwnedKeyFailure<M>] {
        &self.key_failures
    }

    #[must_use]
    pub fn into_parts(self) -> OwnedPartitionParts<M, K, T> {
        (self.buckets, self.key_failures)
    }
}

pub trait PartitionShape<M: IndexDomain>: ElementShape {
    fn member(element: &Self::Element<'_>) -> M::Address;
}

impl<M: IndexDomain, V: ValueDomain> PartitionShape<M> for Indexed<M, V> {
    fn member(element: &Self::Element<'_>) -> M::Address {
        element.0.clone()
    }
}

pub trait PartitionArity<S: ElementShape>: Arity {
    fn into_elements<'a>(
        container: Self::Container<'a, S::Element<'a>>,
    ) -> BoxedIterator<'a, S::Element<'a>>
    where
        S: 'a;

    fn from_bucket<'a>(
        elements: Vec<S::Element<'a>>,
    ) -> QueryResult<Self::Container<'a, S::Element<'a>>>
    where
        S: 'a;
}

impl<S: ElementShape, O: OrderState> PartitionArity<S> for Multiple<O> {
    fn into_elements<'a>(
        container: Self::Container<'a, S::Element<'a>>,
    ) -> BoxedIterator<'a, S::Element<'a>>
    where
        S: 'a,
    {
        container
    }

    fn from_bucket<'a>(
        elements: Vec<S::Element<'a>>,
    ) -> QueryResult<Self::Container<'a, S::Element<'a>>>
    where
        S: 'a,
    {
        Ok(Box::new(elements.into_iter()))
    }
}

impl<S: ElementShape> PartitionArity<S> for Single {
    fn into_elements<'a>(
        container: Self::Container<'a, S::Element<'a>>,
    ) -> BoxedIterator<'a, S::Element<'a>>
    where
        S: 'a,
    {
        Box::new(container.into_iter())
    }

    fn from_bucket<'a>(
        elements: Vec<S::Element<'a>>,
    ) -> QueryResult<Self::Container<'a, S::Element<'a>>>
    where
        S: 'a,
    {
        if elements.len() > 1 {
            return Err(Failure::new(
                InvalidPartitionBucketArity::new("at most one", elements.len()),
                "partition construction",
            ));
        }

        Ok(elements.into_iter().next())
    }
}

impl<S: ElementShape> PartitionArity<S> for Definite {
    fn into_elements<'a>(
        container: Self::Container<'a, S::Element<'a>>,
    ) -> BoxedIterator<'a, S::Element<'a>>
    where
        S: 'a,
    {
        Box::new(std::iter::once(container))
    }

    fn from_bucket<'a>(
        elements: Vec<S::Element<'a>>,
    ) -> QueryResult<Self::Container<'a, S::Element<'a>>>
    where
        S: 'a,
    {
        match <[S::Element<'a>; 1]>::try_from(elements) {
            Ok([element]) => Ok(element),
            Err(elements) => Err(Failure::new(
                InvalidPartitionBucketArity::new("exactly one", elements.len()),
                "partition construction",
            )),
        }
    }
}

pub enum PartitionClassification<K: IndexDomain> {
    Key(K::Owned),
    KeyFailure(Box<Failure>),
    Omit,
}

pub struct PartitionBuilder<'a, M, K, S, C>
where
    M: IndexDomain,
    K: IndexDomain,
    S: PartitionShape<M>,
    C: PartitionArity<S>,
{
    source: C::Container<'a, S::Element<'a>>,
    marker: PhantomData<fn() -> (M, K)>,
}

impl<'a, M, K, S, C> PartitionBuilder<'a, M, K, S, C>
where
    M: IndexDomain,
    K: IndexDomain,
    S: PartitionShape<M>,
    C: PartitionArity<S>,
{
    #[must_use]
    pub fn new(source: C::Container<'a, S::Element<'a>>) -> Self {
        Self {
            source,
            marker: PhantomData,
        }
    }

    pub fn build(
        self,
        graphrecord: &'a GraphRecord,
        mut classify: impl FnMut(&S::Element<'a>) -> PartitionClassification<K>,
    ) -> QueryResult<Partition<'a, M, K, ExpressionHandle<S, C>>> {
        let mut seen_members = GrHashSet::default();
        let mut key_positions: GrHashMap<_, _> = GrHashMap::default();
        let mut buckets = Vec::new();
        let mut key_failures = Vec::new();

        for element in C::into_elements(self.source) {
            let member = S::member(&element);

            if !seen_members.insert(member.clone()) {
                let index = M::index(graphrecord, &member);

                return Err(Failure::new_at::<M, _>(
                    DuplicateIndex::<M>::new(M::own_index(&index)),
                    &index,
                    "partition construction",
                ));
            }

            match classify(&element) {
                PartitionClassification::Key(key) => {
                    let position = if let Some(position) = key_positions.get(&key) {
                        *position
                    } else {
                        let position = buckets.len();
                        key_positions.insert(key.clone(), position);
                        buckets.push((key, Vec::new(), Vec::new()));
                        position
                    };

                    buckets[position].1.push(member);
                    buckets[position].2.push(element);
                }
                PartitionClassification::KeyFailure(failure) => {
                    key_failures.push(KeyFailure { member, failure });
                }
                PartitionClassification::Omit => {}
            }
        }

        let buckets: Vec<_> = buckets
            .into_iter()
            .map(|(key, members, elements)| {
                C::from_bucket(elements).map(|payload| Bucket {
                    key,
                    members,
                    payload: Ok(payload),
                })
            })
            .collect::<QueryResult<_>>()?;

        Ok(Partition {
            buckets,
            key_failures,
        })
    }
}

impl<M: IndexDomain, K: IndexDomain, E: CacheableExpression> CacheableExpression
    for GroupedExpression<M, K, E>
{
    type Cached = OwnedPartition<M, K, E::Cached>;

    fn into_cached(values: Self::ReturnValue<'_>) -> Self::Cached {
        values.into_owned(|payload| payload.map(E::into_cached))
    }

    fn from_cached<'a>(
        graphrecord: &'a GraphRecord,
        cached: &'a Self::Cached,
    ) -> Self::ReturnValue<'a> {
        Partition {
            buckets: cached
                .buckets
                .iter()
                .map(|bucket| Bucket {
                    key: bucket.key.clone(),
                    members: bucket.members.clone(),
                    payload: match &bucket.payload {
                        Ok(payload) => Ok(E::from_cached(graphrecord, payload)),
                        Err(failure) => Err(failure.clone()),
                    },
                })
                .collect(),
            key_failures: cached
                .key_failures
                .iter()
                .map(|key_failure| KeyFailure {
                    member: key_failure.member.clone(),
                    failure: key_failure.failure.clone(),
                })
                .collect(),
        }
    }
}
