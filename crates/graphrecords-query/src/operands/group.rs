use crate::{
    Arity, BoxedIterator, Definite, ElementShape, EvaluateOperand, Failure, IndexDomain, Indexed,
    Multiple, Operand, OperandContext, OrderState, QueryResult, Single, ValueDomain,
    error::{grouping::InvalidPartitionBucketArity, index::DuplicateIndex},
    execution::{CacheableOperand, EvaluationCache},
    index::GroupKey,
    operands::OperandHandle,
    optimizer::{Estimate, Estimated, PlanNode, Stats},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::{GrHashMap, GrHashSet};
use std::{marker::PhantomData, sync::Arc};

pub struct GroupOperand<M: IndexDomain, K: GroupKey, O: Operand> {
    context: Arc<dyn OperandContext<Self>>,
}

impl<M: IndexDomain, K: GroupKey, O: Operand> Clone for GroupOperand<M, K, O> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<M: IndexDomain, K: GroupKey, O: Operand> EvaluateOperand for GroupOperand<M, K, O> {
    type ReturnValue<'a>
        = Partition<'a, M, K, O>
    where
        Self: 'a;

    fn evaluate<'a>(
        &'a self,
        graphrecord: &'a GraphRecord,
        cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::ReturnValue<'a>> {
        self.context.evaluate(graphrecord, cache)
    }
}

impl<M: IndexDomain, K: GroupKey, O: Operand> Estimated for GroupOperand<M, K, O> {
    fn estimate(&self, stats: &Stats) -> Estimate {
        self.context().estimate(stats)
    }
}

impl<M: IndexDomain, K: GroupKey, O: Operand> Operand for GroupOperand<M, K, O> {
    fn context(&self) -> &dyn OperandContext<Self> {
        self.context.as_ref()
    }

    fn as_plan_node(&self) -> &dyn PlanNode {
        self.context.as_ref()
    }

    fn from_context(context: Arc<dyn OperandContext<Self>>) -> Self {
        Self { context }
    }
}

pub struct Partition<'a, M: IndexDomain, K: GroupKey, O: Operand> {
    buckets: Vec<Bucket<'a, M, K, O>>,
    key_failures: Vec<KeyFailure<'a, M>>,
}

pub struct Bucket<'a, M: IndexDomain, K: GroupKey, O: Operand> {
    key: K::Owned,
    members: Vec<M::Index<'a>>,
    payload: QueryResult<O::ReturnValue<'a>>,
}

impl<'a, M: IndexDomain, K: GroupKey, O: Operand> Bucket<'a, M, K, O> {
    #[must_use]
    pub const fn key(&self) -> &K::Owned {
        &self.key
    }

    #[must_use]
    pub fn members(&self) -> &[M::Index<'a>] {
        &self.members
    }

    pub const fn payload(&self) -> &QueryResult<O::ReturnValue<'a>> {
        &self.payload
    }
}

pub struct KeyFailure<'a, M: IndexDomain> {
    member: M::Index<'a>,
    failure: Box<Failure>,
}

impl<'a, M: IndexDomain> KeyFailure<'a, M> {
    #[must_use]
    pub const fn member(&self) -> &M::Index<'a> {
        &self.member
    }

    #[must_use]
    pub fn failure(&self) -> &Failure {
        &self.failure
    }
}

pub type PartitionBucketParts<'a, M, K, O> = (
    <K as IndexDomain>::Owned,
    Vec<<M as IndexDomain>::Index<'a>>,
    QueryResult<<O as EvaluateOperand>::ReturnValue<'a>>,
);
pub type PartitionKeyFailureParts<'a, M> = (<M as IndexDomain>::Index<'a>, Box<Failure>);
pub type PartitionParts<'a, M, K, O> = (
    Vec<PartitionBucketParts<'a, M, K, O>>,
    Vec<PartitionKeyFailureParts<'a, M>>,
);

pub enum BucketChange<'a, O: Operand> {
    Drop,
    ReplacePayload(QueryResult<O::ReturnValue<'a>>),
}

pub enum KeyFailureChange {
    Drop,
    Raise,
}

impl<'a, M: IndexDomain, K: GroupKey, O: Operand> Partition<'a, M, K, O> {
    #[must_use]
    pub fn buckets(&self) -> impl ExactSizeIterator<Item = &Bucket<'a, M, K, O>> + '_ {
        self.buckets.iter()
    }

    #[must_use]
    pub fn key_failures(&self) -> impl ExactSizeIterator<Item = &KeyFailure<'a, M>> + '_ {
        self.key_failures.iter()
    }

    pub fn map_payloads<N: Operand>(
        self,
        mut function: impl FnMut(
            &K::Owned,
            &[M::Index<'a>],
            QueryResult<O::ReturnValue<'a>>,
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
    pub(crate) fn map_domains<N, L>(
        self,
        mut map_member: impl FnMut(M::Index<'a>) -> N::Index<'a>,
        mut map_key: impl FnMut(K::Owned) -> L::Owned,
    ) -> Partition<'a, N, L, O>
    where
        N: IndexDomain,
        L: GroupKey,
    {
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
        mut function: impl FnMut(&Bucket<'a, M, K, O>) -> Option<BucketChange<'a, O>>,
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
        mut function: impl FnMut(&KeyFailure<'a, M>) -> Option<KeyFailureChange>,
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
    pub fn into_parts(self) -> PartitionParts<'a, M, K, O> {
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

    pub fn into_return_partition<T>(
        self,
        mut convert_payload: impl FnMut(QueryResult<O::ReturnValue<'a>>) -> QueryResult<T>,
    ) -> ReturnPartition<'a, M, K, T> {
        ReturnPartition {
            buckets: self
                .buckets
                .into_iter()
                .map(|bucket| ReturnBucket {
                    key: bucket.key,
                    members: bucket.members,
                    payload: convert_payload(bucket.payload),
                })
                .collect(),
            key_failures: self
                .key_failures
                .into_iter()
                .map(|key_failure| ReturnKeyFailure {
                    member: key_failure.member,
                    failure: key_failure.failure,
                })
                .collect(),
        }
    }

    pub fn into_owned<T>(
        self,
        mut convert_payload: impl FnMut(QueryResult<O::ReturnValue<'a>>) -> QueryResult<T>,
    ) -> PartitionOwned<M, K, T> {
        PartitionOwned {
            buckets: self
                .buckets
                .into_iter()
                .map(|bucket| BucketOwned {
                    key: bucket.key,
                    members: bucket
                        .members
                        .into_iter()
                        .map(|member| M::to_owned(&member))
                        .collect(),
                    payload: convert_payload(bucket.payload),
                })
                .collect(),
            key_failures: self
                .key_failures
                .into_iter()
                .map(|key_failure| KeyFailureOwned {
                    member: M::to_owned(&key_failure.member),
                    failure: key_failure.failure,
                })
                .collect(),
        }
    }
}

pub struct ReturnPartition<'a, M: IndexDomain, K: GroupKey, T> {
    buckets: Vec<ReturnBucket<'a, M, K, T>>,
    key_failures: Vec<ReturnKeyFailure<'a, M>>,
}

pub struct ReturnBucket<'a, M: IndexDomain, K: GroupKey, T> {
    key: K::Owned,
    members: Vec<M::Index<'a>>,
    payload: QueryResult<T>,
}

impl<'a, M: IndexDomain, K: GroupKey, T> ReturnBucket<'a, M, K, T> {
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

impl<'a, M: IndexDomain, K: GroupKey, T> ReturnPartition<'a, M, K, T> {
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

pub struct PartitionOwned<M: IndexDomain, K: GroupKey, T> {
    buckets: Vec<BucketOwned<M, K, T>>,
    key_failures: Vec<KeyFailureOwned<M>>,
}

pub struct BucketOwned<M: IndexDomain, K: GroupKey, T> {
    key: K::Owned,
    members: Vec<M::Owned>,
    payload: QueryResult<T>,
}

impl<M: IndexDomain, K: GroupKey, T> BucketOwned<M, K, T> {
    #[must_use]
    pub const fn key(&self) -> &K::Owned {
        &self.key
    }

    #[must_use]
    pub fn members(&self) -> &[M::Owned] {
        &self.members
    }

    pub const fn payload(&self) -> &QueryResult<T> {
        &self.payload
    }

    pub fn into_parts(self) -> (K::Owned, Vec<M::Owned>, QueryResult<T>) {
        (self.key, self.members, self.payload)
    }
}

pub struct KeyFailureOwned<M: IndexDomain> {
    member: M::Owned,
    failure: Box<Failure>,
}

impl<M: IndexDomain> KeyFailureOwned<M> {
    #[must_use]
    pub const fn member(&self) -> &M::Owned {
        &self.member
    }

    #[must_use]
    pub fn failure(&self) -> &Failure {
        &self.failure
    }

    #[must_use]
    pub fn into_parts(self) -> (M::Owned, Box<Failure>) {
        (self.member, self.failure)
    }
}

pub type PartitionOwnedParts<M, K, T> = (Vec<BucketOwned<M, K, T>>, Vec<KeyFailureOwned<M>>);

impl<M: IndexDomain, K: GroupKey, T> PartitionOwned<M, K, T> {
    #[must_use]
    pub fn buckets(&self) -> &[BucketOwned<M, K, T>] {
        &self.buckets
    }

    #[must_use]
    pub fn key_failures(&self) -> &[KeyFailureOwned<M>] {
        &self.key_failures
    }

    #[must_use]
    pub fn into_parts(self) -> PartitionOwnedParts<M, K, T> {
        (self.buckets, self.key_failures)
    }
}

pub trait PartitionShape<M: IndexDomain>: ElementShape {
    fn member<'a>(element: &Self::Element<'a>) -> M::Index<'a>;
}

impl<M: IndexDomain, V: ValueDomain> PartitionShape<M> for Indexed<M, V> {
    fn member<'a>(element: &Self::Element<'a>) -> M::Index<'a> {
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
                "partition construction",
                InvalidPartitionBucketArity::new("exactly one", elements.len()),
            )),
        }
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
                "partition construction",
                InvalidPartitionBucketArity::new("at most one", elements.len()),
            ));
        }

        Ok(elements.into_iter().next())
    }
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

pub enum PartitionClassification<K: GroupKey> {
    Key(K::Owned),
    KeyFailure(Box<Failure>),
    Omit,
}

pub struct PartitionBuilder<'a, M, K, S, C>
where
    M: IndexDomain,
    K: GroupKey,
    S: PartitionShape<M>,
    C: PartitionArity<S>,
{
    source: C::Container<'a, S::Element<'a>>,
    marker: PhantomData<fn() -> (M, K)>,
}

impl<'a, M, K, S, C> PartitionBuilder<'a, M, K, S, C>
where
    M: IndexDomain,
    K: GroupKey,
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
        mut classify: impl FnMut(&S::Element<'a>) -> PartitionClassification<K>,
    ) -> QueryResult<Partition<'a, M, K, OperandHandle<S, C>>> {
        let mut seen_members = GrHashSet::default();
        let mut key_positions: GrHashMap<_, _> = GrHashMap::default();
        let mut buckets = Vec::new();
        let mut key_failures = Vec::new();

        for element in C::into_elements(self.source) {
            let member = S::member(&element);

            if !seen_members.insert(M::to_owned(&member)) {
                return Err(Failure::new_at::<M, _>(
                    "partition construction",
                    DuplicateIndex::<M>::new(M::to_owned(&member)),
                    &member,
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

        let buckets = buckets
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

impl<M: IndexDomain, K: GroupKey, O: CacheableOperand> CacheableOperand for GroupOperand<M, K, O> {
    type Cached = PartitionOwned<M, K, O::Cached>;

    fn into_cached(values: Self::ReturnValue<'_>) -> Self::Cached {
        values.into_owned(|payload| payload.map(O::into_cached))
    }

    fn from_cached(cached: &Self::Cached) -> Self::ReturnValue<'_> {
        Partition {
            buckets: cached
                .buckets
                .iter()
                .map(|bucket| Bucket {
                    key: bucket.key.clone(),
                    members: bucket.members.iter().map(M::from_owned).collect(),
                    payload: match &bucket.payload {
                        Ok(payload) => Ok(O::from_cached(payload)),
                        Err(failure) => Err(failure.clone()),
                    },
                })
                .collect(),
            key_failures: cached
                .key_failures
                .iter()
                .map(|key_failure| KeyFailure {
                    member: M::from_owned(&key_failure.member),
                    failure: key_failure.failure.clone(),
                })
                .collect(),
        }
    }
}
