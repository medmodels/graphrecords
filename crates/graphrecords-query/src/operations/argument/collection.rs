use crate::{
    Explain, IndexValue, QueryResult, Scalar, ValueDomain,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    operations::{Prepare, SetSource},
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use chrono::{NaiveDateTime, TimeDelta};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{
        AttributeName, GroupIndex, GroupIndexView, IdentifierView, NodeIndex, NodeIndexView, Value,
        ValueView, datatypes::AttributeNameView,
    },
};
use graphrecords_utils::aliases::GrHashSet;
use std::{
    borrow::Cow,
    collections::HashSet,
    fmt::{self, Display, Write},
    hash::{BuildHasher, DefaultHasher, Hash, Hasher},
};

macro_rules! literal_set {
    ($source:ty, $domain:ty, | $member:ident | $value:expr) => {
        impl SetSource<$domain> for Vec<$source> {
            fn set<'a>(
                _graphrecord: &'a GraphRecord,
                prepared: Self::Prepared<'a>,
                _label: &'static str,
            ) -> QueryResult<GrHashSet<<$domain as ValueDomain>::Value<'a>>>
            where
                Self: 'a,
                <$domain as ValueDomain>::Value<'a>: Eq + Hash,
            {
                Ok(prepared.iter().map(|$member| $value).collect())
            }
        }

        impl<const N: usize> SetSource<$domain> for [$source; N] {
            fn set<'a>(
                _graphrecord: &'a GraphRecord,
                prepared: Self::Prepared<'a>,
                _label: &'static str,
            ) -> QueryResult<GrHashSet<<$domain as ValueDomain>::Value<'a>>>
            where
                Self: 'a,
                <$domain as ValueDomain>::Value<'a>: Eq + Hash,
            {
                Ok(prepared.iter().map(|$member| $value).collect())
            }
        }
    };
}

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

literal_set!(bool, Scalar, |member| ValueView::Bool(*member));
literal_set!(bool, IndexValue<Value>, |member| Value::Bool(*member));

literal_set!(i64, Scalar, |member| ValueView::Int(*member));
literal_set!(i64, IndexValue<NodeIndex>, |member| NodeIndexView::from(
    IdentifierView::Int(*member)
));
literal_set!(i64, IndexValue<GroupIndex>, |member| GroupIndexView::from(
    IdentifierView::Int(*member)
));
literal_set!(i64, AttributeName, |member| AttributeNameView::from(
    IdentifierView::Int(*member)
));
literal_set!(i64, IndexValue<AttributeName>, |member| {
    AttributeNameView::from(IdentifierView::Int(*member))
});
literal_set!(i64, IndexValue<Value>, |member| Value::Int(*member));

literal_set!(&'static str, Scalar, |member| ValueView::String(
    Cow::Borrowed(*member)
));
literal_set!(&'static str, IndexValue<NodeIndex>, |member| {
    NodeIndexView::from(IdentifierView::String(Cow::Borrowed(*member)))
});
literal_set!(&'static str, IndexValue<GroupIndex>, |member| {
    GroupIndexView::from(IdentifierView::String(Cow::Borrowed(*member)))
});
literal_set!(&'static str, AttributeName, |member| {
    AttributeNameView::from(IdentifierView::String(Cow::Borrowed(*member)))
});
literal_set!(&'static str, IndexValue<AttributeName>, |member| {
    AttributeNameView::from(IdentifierView::String(Cow::Borrowed(*member)))
});
literal_set!(&'static str, IndexValue<Value>, |member| Value::String(
    (*member).to_string()
));

literal_set!(String, Scalar, |member| ValueView::String(Cow::Borrowed(
    member.as_str()
)));
literal_set!(String, IndexValue<NodeIndex>, |member| NodeIndexView::from(
    IdentifierView::String(Cow::Borrowed(member.as_str()))
));
literal_set!(String, IndexValue<GroupIndex>, |member| {
    GroupIndexView::from(IdentifierView::String(Cow::Borrowed(member.as_str())))
});
literal_set!(String, AttributeName, |member| AttributeNameView::from(
    IdentifierView::String(Cow::Borrowed(member.as_str()))
));
literal_set!(String, IndexValue<AttributeName>, |member| {
    AttributeNameView::from(IdentifierView::String(Cow::Borrowed(member.as_str())))
});
literal_set!(String, IndexValue<Value>, |member| Value::String(
    member.clone()
));

literal_set!(NaiveDateTime, Scalar, |member| ValueView::DateTime(*member));
literal_set!(NaiveDateTime, IndexValue<Value>, |member| Value::DateTime(
    *member
));

literal_set!(TimeDelta, Scalar, |member| ValueView::Duration(*member));
literal_set!(TimeDelta, IndexValue<Value>, |member| Value::Duration(
    *member
));

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
