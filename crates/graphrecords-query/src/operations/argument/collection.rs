use crate::{
    Explain, QueryResult, ValueDomain,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{Prepare, SetSource},
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use graphrecords_core::GraphRecord;
use graphrecords_utils::aliases::GrHashSet;
use std::{
    collections::HashSet,
    fmt::{self, Display, Write},
    hash::{BuildHasher, DefaultHasher, Hash, Hasher},
};

impl<T: Display> Explain for Vec<T> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.write_char('[')?;

        for (position, member) in self.iter().enumerate() {
            if position > 0 {
                formatter.write_str(", ")?;
            }

            write!(formatter, "{member}")?;
        }

        formatter.write_char(']')
    }
}

impl<T: PartialEq + Hash> PlanIdentity for Vec<T> {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl<T: Clone> PlanInputs for Vec<T> {}

impl<T: 'static + Send + Sync> Prepare for Vec<T> {
    type Prepared<'a> = &'a [T];

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T> Estimated for Vec<T> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate {
            elements: Some(self.len()),
            ..Estimate::UNKNOWN
        }
    }
}

impl<T, V> SetSource<V> for Vec<T>
where
    T: 'static + Clone + Eq + Hash + Display + Send + Sync,
    V: ValueDomain<Owned = T>,
{
    fn set<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
        label: &'static str,
    ) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash,
    {
        prepared
            .iter()
            .map(|owned| V::from_owned(graphrecord, owned, label))
            .collect()
    }
}

impl<T: Display, const N: usize> Explain for [T; N] {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        formatter.write_char('[')?;

        for (position, member) in self.iter().enumerate() {
            if position > 0 {
                formatter.write_str(", ")?;
            }

            write!(formatter, "{member}")?;
        }

        formatter.write_char(']')
    }
}

impl<T: PartialEq + Hash, const N: usize> PlanIdentity for [T; N] {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl<T: Clone, const N: usize> PlanInputs for [T; N] {}

impl<T: 'static + Send + Sync, const N: usize> Prepare for [T; N] {
    type Prepared<'a> = &'a [T];

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T, const N: usize> Estimated for [T; N] {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate {
            elements: Some(N),
            ..Estimate::UNKNOWN
        }
    }
}

impl<T, V, const N: usize> SetSource<V> for [T; N]
where
    T: 'static + Clone + Eq + Hash + Display + Send + Sync,
    V: ValueDomain<Owned = T>,
{
    fn set<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
        label: &'static str,
    ) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash,
    {
        prepared
            .iter()
            .map(|owned| V::from_owned(graphrecord, owned, label))
            .collect()
    }
}

impl<T: Display> Explain for GrHashSet<T> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        let mut members: Vec<_> = self.iter().map(ToString::to_string).collect();
        members.sort_unstable();

        formatter.write_char('[')?;

        for (position, member) in members.iter().enumerate() {
            if position > 0 {
                formatter.write_str(", ")?;
            }

            formatter.write_str(member)?;
        }

        formatter.write_char(']')
    }
}

impl<T: Eq + Hash> PlanIdentity for GrHashSet<T> {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.len());

        let combined = self
            .iter()
            .map(|member| {
                let mut hasher = DefaultHasher::new();
                member.hash(&mut hasher);
                hasher.finish()
            })
            .fold(0_u64, u64::wrapping_add);

        state.write_u64(combined);
    }
}

impl<T: Clone> PlanInputs for GrHashSet<T> {}

impl<T: 'static + Send + Sync> Prepare for GrHashSet<T> {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T> Estimated for GrHashSet<T> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::values(self.len(), self.len())
    }
}

impl<T, V> SetSource<V> for GrHashSet<T>
where
    T: 'static + Clone + Eq + Hash + Display + Send + Sync,
    V: ValueDomain<Owned = T>,
{
    fn set<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
        label: &'static str,
    ) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash,
    {
        prepared
            .iter()
            .map(|owned| V::from_owned(graphrecord, owned, label))
            .collect()
    }
}

impl<T: Display, S> Explain for HashSet<T, S> {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        let mut members: Vec<_> = self.iter().map(ToString::to_string).collect();
        members.sort_unstable();

        formatter.write_char('[')?;

        for (position, member) in members.iter().enumerate() {
            if position > 0 {
                formatter.write_str(", ")?;
            }

            formatter.write_str(member)?;
        }

        formatter.write_char(']')
    }
}

impl<T: Eq + Hash, S: BuildHasher> PlanIdentity for HashSet<T, S> {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.len());

        let combined = self
            .iter()
            .map(|member| {
                let mut hasher = DefaultHasher::new();
                member.hash(&mut hasher);
                hasher.finish()
            })
            .fold(0_u64, u64::wrapping_add);

        state.write_u64(combined);
    }
}

impl<T: Clone, S: Clone> PlanInputs for HashSet<T, S> {}

impl<T: 'static + Send + Sync, S: 'static + Send + Sync> Prepare for HashSet<T, S> {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T, S> Estimated for HashSet<T, S> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::values(self.len(), self.len())
    }
}

impl<T, S, V> SetSource<V> for HashSet<T, S>
where
    T: 'static + Clone + Eq + Hash + Display + Send + Sync,
    V: ValueDomain<Owned = T>,
    S: 'static + Clone + BuildHasher + Send + Sync,
{
    fn set<'a>(
        graphrecord: &'a GraphRecord,
        prepared: Self::Prepared<'a>,
        label: &'static str,
    ) -> QueryResult<GrHashSet<V::Value<'a>>>
    where
        Self: 'a,
        V::Value<'a>: Eq + Hash,
    {
        prepared
            .iter()
            .map(|owned| V::from_owned(graphrecord, owned, label))
            .collect()
    }
}
