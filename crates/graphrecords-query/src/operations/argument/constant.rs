use crate::{
    Explain, FailureKind, FailureKindValue, IndexValue, Mask, Position, QueryResult, Scalar,
    ValueDomain,
    element::Preserving,
    execution::EvaluationCache,
    explain::ExplainFormatter,
    index::Positional,
    operations::{Alignment, ArgumentSource, Lookup, Prepare, SourceDomain},
    optimizer::{Estimate, Estimated, PlanIdentity, PlanInputs, Stats},
};
use chrono::{NaiveDateTime, TimeDelta};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{
        AttributeName, EdgeIndex, GroupIndex, GroupIndexView, IdentifierView, NodeIndex,
        NodeIndexView, Value, ValueView, datatypes::AttributeNameView,
    },
};
use std::{
    borrow::Cow,
    fmt::{self, Write},
    hash::{Hash, Hasher},
};

macro_rules! literal_argument {
    ($source:ty, $domain:ty, | $prepared:ident | $value:expr) => {
        impl<A: Alignment> ArgumentSource<A, $domain> for $source {
            type Retention = Preserving;

            fn lookup<'a>(
                _graphrecord: &'a GraphRecord,
                $prepared: &Self::Prepared<'a>,
                _address: &A::Address,
                _label: &'static str,
            ) -> Lookup<QueryResult<<$domain as ValueDomain>::Value<'a>>>
            where
                Self: 'a,
            {
                Lookup::Present(Ok($value))
            }
        }
    };
}

impl Explain for Value {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for Value {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for Value {}

impl Prepare for Value {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for Value {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for Value {
    type ValueDomain = Scalar;
}

impl<A, V> ArgumentSource<A, V> for Value
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

impl Explain for bool {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for bool {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for bool {}

impl Prepare for bool {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for bool {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate {
            selectivity: Some(if *self { 1.0 } else { 0.0 }),
            ..Estimate::singleton()
        }
    }
}

impl SourceDomain for bool {
    type ValueDomain = Mask;
}

impl<A, V> ArgumentSource<A, V> for bool
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

literal_argument!(bool, Scalar, |prepared| ValueView::Bool(**prepared));
literal_argument!(bool, IndexValue<Value>, |prepared| Value::Bool(**prepared));

impl Explain for i64 {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for i64 {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for i64 {}

impl Prepare for i64 {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for i64 {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for i64 {
    type ValueDomain = Scalar;
}

literal_argument!(i64, Scalar, |prepared| ValueView::Int(**prepared));
literal_argument!(i64, IndexValue<NodeIndex>, |prepared| NodeIndexView::from(
    IdentifierView::Int(**prepared)
));
literal_argument!(
    i64,
    IndexValue<GroupIndex>,
    |prepared| GroupIndexView::from(IdentifierView::Int(**prepared))
);
literal_argument!(i64, AttributeName, |prepared| AttributeNameView::from(
    IdentifierView::Int(**prepared)
));
literal_argument!(i64, IndexValue<AttributeName>, |prepared| {
    AttributeNameView::from(IdentifierView::Int(**prepared))
});
literal_argument!(i64, IndexValue<Value>, |prepared| Value::Int(**prepared));

impl Explain for f64 {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for f64 {
    fn identity_eq(&self, other: &Self) -> bool {
        if self.is_nan() {
            other.is_nan()
        } else {
            self == other
        }
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        let collapsed = if self.is_nan() {
            Self::NAN
        } else if *self == 0.0 {
            0.0_f64
        } else {
            *self
        };

        collapsed.to_bits().hash(state);
    }
}

impl PlanInputs for f64 {}

impl Prepare for f64 {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for f64 {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for f64 {
    type ValueDomain = Scalar;
}

literal_argument!(f64, Scalar, |prepared| ValueView::Float(**prepared));
literal_argument!(f64, IndexValue<Value>, |prepared| Value::Float(**prepared));

impl Explain for &'static str {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "\"{self}\"")
    }
}

impl PlanIdentity for &'static str {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for &'static str {}

impl Prepare for &'static str {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for &'static str {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for &'static str {
    type ValueDomain = Scalar;
}

literal_argument!(&'static str, Scalar, |prepared| ValueView::String(
    Cow::Borrowed(**prepared)
));
literal_argument!(&'static str, IndexValue<NodeIndex>, |prepared| {
    NodeIndexView::from(IdentifierView::String(Cow::Borrowed(**prepared)))
});
literal_argument!(&'static str, IndexValue<GroupIndex>, |prepared| {
    GroupIndexView::from(IdentifierView::String(Cow::Borrowed(**prepared)))
});
literal_argument!(&'static str, AttributeName, |prepared| {
    AttributeNameView::from(IdentifierView::String(Cow::Borrowed(**prepared)))
});
literal_argument!(&'static str, IndexValue<AttributeName>, |prepared| {
    AttributeNameView::from(IdentifierView::String(Cow::Borrowed(**prepared)))
});
literal_argument!(&'static str, IndexValue<Value>, |prepared| Value::String(
    (**prepared).to_string()
));

impl Explain for String {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "\"{self}\"")
    }
}

impl PlanIdentity for String {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for String {}

impl Prepare for String {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for String {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for String {
    type ValueDomain = Scalar;
}

literal_argument!(String, Scalar, |prepared| ValueView::String(Cow::Borrowed(
    prepared.as_str()
)));
literal_argument!(String, IndexValue<NodeIndex>, |prepared| {
    NodeIndexView::from(IdentifierView::String(Cow::Borrowed(prepared.as_str())))
});
literal_argument!(String, IndexValue<GroupIndex>, |prepared| {
    GroupIndexView::from(IdentifierView::String(Cow::Borrowed(prepared.as_str())))
});
literal_argument!(String, AttributeName, |prepared| AttributeNameView::from(
    IdentifierView::String(Cow::Borrowed(prepared.as_str()))
));
literal_argument!(String, IndexValue<AttributeName>, |prepared| {
    AttributeNameView::from(IdentifierView::String(Cow::Borrowed(prepared.as_str())))
});
literal_argument!(String, IndexValue<Value>, |prepared| Value::String(
    (*prepared).clone()
));

impl Explain for NaiveDateTime {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for NaiveDateTime {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for NaiveDateTime {}

impl Prepare for NaiveDateTime {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for NaiveDateTime {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for NaiveDateTime {
    type ValueDomain = Scalar;
}

literal_argument!(NaiveDateTime, Scalar, |prepared| ValueView::DateTime(
    **prepared
));
literal_argument!(NaiveDateTime, IndexValue<Value>, |prepared| {
    Value::DateTime(**prepared)
});

impl Explain for TimeDelta {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for TimeDelta {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for TimeDelta {}

impl Prepare for TimeDelta {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for TimeDelta {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for TimeDelta {
    type ValueDomain = Scalar;
}

literal_argument!(TimeDelta, Scalar, |prepared| ValueView::Duration(
    **prepared
));
literal_argument!(TimeDelta, IndexValue<Value>, |prepared| Value::Duration(
    **prepared
));

impl Explain for AttributeName {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for AttributeName {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for AttributeName {}

impl Prepare for AttributeName {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for AttributeName {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for AttributeName {
    type ValueDomain = Self;
}

impl<A, V> ArgumentSource<A, V> for AttributeName
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

impl Explain for EdgeIndex {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for EdgeIndex {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for EdgeIndex {}

impl Prepare for EdgeIndex {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for EdgeIndex {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for EdgeIndex {
    type ValueDomain = IndexValue<Self>;
}

impl<A, V> ArgumentSource<A, V> for EdgeIndex
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

impl Explain for Position {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for Position {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for Position {}

impl Prepare for Position {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for Position {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for Position {
    type ValueDomain = IndexValue<Positional>;
}

impl<A, V> ArgumentSource<A, V> for Position
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

impl Explain for FailureKind {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for FailureKind {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for FailureKind {}

impl Prepare for FailureKind {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for FailureKind {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for FailureKind {
    type ValueDomain = FailureKindValue;
}

impl<A, V> ArgumentSource<A, V> for FailureKind
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

impl Explain for NodeIndex {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for NodeIndex {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for NodeIndex {}

impl Prepare for NodeIndex {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for NodeIndex {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for NodeIndex {
    type ValueDomain = IndexValue<Self>;
}

impl<A, V> ArgumentSource<A, V> for NodeIndex
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}

impl Explain for GroupIndex {
    fn describe<'a>(&'a self, formatter: &mut ExplainFormatter<'a, '_>) -> fmt::Result {
        write!(formatter, "{self}")
    }
}

impl PlanIdentity for GroupIndex {
    fn identity_eq(&self, other: &Self) -> bool {
        self == other
    }

    fn identity_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl PlanInputs for GroupIndex {}

impl Prepare for GroupIndex {
    type Prepared<'a> = &'a Self;

    fn prepare<'a>(
        &'a self,
        _graphrecord: &'a GraphRecord,
        _cache: &'a EvaluationCache,
    ) -> QueryResult<Self::Prepared<'a>> {
        Ok(self)
    }
}

impl Estimated for GroupIndex {
    fn estimate(&self, _stats: &Stats) -> Estimate {
        Estimate::singleton()
    }
}

impl SourceDomain for GroupIndex {
    type ValueDomain = IndexValue<Self>;
}

impl<A, V> ArgumentSource<A, V> for GroupIndex
where
    A: Alignment,
    V: ValueDomain<Owned = Self>,
{
    type Retention = Preserving;

    fn lookup<'a>(
        graphrecord: &'a GraphRecord,
        prepared: &Self::Prepared<'a>,
        _address: &A::Address,
        label: &'static str,
    ) -> Lookup<QueryResult<V::Value<'a>>>
    where
        Self: 'a,
    {
        Lookup::Present(V::from_owned(graphrecord, *prepared, label))
    }
}
