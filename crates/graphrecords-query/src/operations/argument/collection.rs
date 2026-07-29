use crate::{
    Explain, QueryResult,
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
        Hash::hash(self, state);
    }
}

impl<T: Clone> PlanInputs for Vec<T> {}

impl<T: 'static> Prepare for Vec<T> {
    type Prepared<'a> = &'a [T];

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
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

impl<T> SetSource for Vec<T>
where
    T: 'static + Clone + Eq + Hash + Display,
{
    type Value<'a> = T;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Ok(prepared.iter().cloned().collect())
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
        Hash::hash(self, state);
    }
}

impl<T: Clone, const N: usize> PlanInputs for [T; N] {}

impl<T: 'static, const N: usize> Prepare for [T; N] {
    type Prepared<'a> = &'a [T];

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
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

impl<T, const N: usize> SetSource for [T; N]
where
    T: 'static + Clone + Eq + Hash + Display,
{
    type Value<'a> = T;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Ok(prepared.iter().cloned().collect())
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

impl<T: 'static> Prepare for GrHashSet<T> {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T> Estimated for GrHashSet<T> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::values(self.len(), self.len())
    }
}

impl<T> SetSource for GrHashSet<T>
where
    T: 'static + Clone + Eq + Hash + Display,
{
    type Value<'a> = T;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Ok(prepared.clone())
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

impl<T: 'static, S: 'static> Prepare for HashSet<T, S> {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache<'a>,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl<T, S> Estimated for HashSet<T, S> {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::values(self.len(), self.len())
    }
}

impl<T, S> SetSource for HashSet<T, S>
where
    T: 'static + Clone + Eq + Hash + Display,
    S: 'static + Clone + BuildHasher,
{
    type Value<'a> = T;

    fn set<'a>(prepared: Self::Prepared<'a>) -> QueryResult<GrHashSet<Self::Value<'a>>>
    where
        Self: 'a,
    {
        Ok(prepared.iter().cloned().collect())
    }
}
