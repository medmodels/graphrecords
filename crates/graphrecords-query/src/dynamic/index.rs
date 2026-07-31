use crate::{
    AttributeName, EdgeEndpointRole, FailureKind, IndexDomain, Position, Positional, QueryResult,
    capabilities::{EnsureSortable, incomparable_pair},
    index::GroupKey,
    registry::IndexDescriptor,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{EdgeIndex, GraphRecordAttribute, GraphRecordValue, NodeIndex},
};
use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
};

#[derive(Clone)]
pub struct DynIndex;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DynIndexOwned {
    Positional(Position),
    Node(NodeIndex),
    Edge(EdgeIndex),
    Attribute(GraphRecordAttribute),
    Value(GraphRecordValue),
    Bool(bool),
    EndpointRole(EdgeEndpointRole),
    FailureKind(FailureKind),
    Expanded(Box<DynExpandedOwned>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DynIndexRef<'a> {
    Positional(Position),
    Node(&'a NodeIndex),
    Edge(&'a EdgeIndex),
    Attribute(GraphRecordAttribute),
    Value(GraphRecordValue),
    Bool(bool),
    EndpointRole(EdgeEndpointRole),
    FailureKind(FailureKind),
    Expanded(Box<DynExpandedRef<'a>>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DynExpandedOwned {
    parent: DynIndexOwned,
    child: Option<DynIndexOwned>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DynExpandedRef<'a> {
    parent: DynIndexRef<'a>,
    child: Option<DynIndexRef<'a>>,
}

impl DynExpandedOwned {
    #[must_use]
    pub const fn parent_index(&self) -> &DynIndexOwned {
        &self.parent
    }

    #[must_use]
    pub const fn child_index(&self) -> Option<&DynIndexOwned> {
        self.child.as_ref()
    }

    #[must_use]
    pub fn into_parts(self) -> (DynIndexOwned, Option<DynIndexOwned>) {
        (self.parent, self.child)
    }

    #[must_use]
    pub const fn is_source(&self) -> bool {
        self.child.is_none()
    }

    #[must_use]
    pub const fn source(parent: DynIndexOwned) -> Self {
        Self {
            parent,
            child: None,
        }
    }

    #[must_use]
    pub const fn child(parent: DynIndexOwned, child: DynIndexOwned) -> Self {
        Self {
            parent,
            child: Some(child),
        }
    }
}

impl<'a> DynExpandedRef<'a> {
    #[must_use]
    pub const fn parent_index(&self) -> &DynIndexRef<'a> {
        &self.parent
    }

    #[must_use]
    pub const fn child_index(&self) -> Option<&DynIndexRef<'a>> {
        self.child.as_ref()
    }

    #[must_use]
    pub fn into_parts(self) -> (DynIndexRef<'a>, Option<DynIndexRef<'a>>) {
        (self.parent, self.child)
    }

    #[must_use]
    pub const fn is_source(&self) -> bool {
        self.child.is_none()
    }

    #[must_use]
    pub const fn source(parent: DynIndexRef<'a>) -> Self {
        Self {
            parent,
            child: None,
        }
    }

    #[must_use]
    pub const fn child(parent: DynIndexRef<'a>, child: DynIndexRef<'a>) -> Self {
        Self {
            parent,
            child: Some(child),
        }
    }
}

impl DynIndexOwned {
    pub(crate) fn descriptor(&self) -> IndexDescriptor {
        match self {
            Self::Positional(_) => IndexDescriptor::domain::<Positional>(),
            Self::Node(_) => IndexDescriptor::domain::<NodeIndex>(),
            Self::Edge(_) => IndexDescriptor::domain::<EdgeIndex>(),
            Self::Attribute(_) => IndexDescriptor::domain::<AttributeName>(),
            Self::Value(_) => IndexDescriptor::domain::<GraphRecordValue>(),
            Self::Bool(_) => IndexDescriptor::domain::<bool>(),
            Self::EndpointRole(_) => IndexDescriptor::domain::<EdgeEndpointRole>(),
            Self::FailureKind(_) => IndexDescriptor::domain::<FailureKind>(),
            Self::Expanded(expanded) => {
                let parent = expanded.parent_index().descriptor();

                expanded.child_index().map_or_else(
                    || IndexDescriptor::expanded_source(parent.clone()),
                    |child| IndexDescriptor::expanded(parent.clone(), child.descriptor()),
                )
            }
        }
    }

    pub(crate) const fn description(&self) -> &'static str {
        match self {
            Self::Positional(_) => "positional index",
            Self::Node(_) => "node index",
            Self::Edge(_) => "edge index",
            Self::Attribute(_) => "attribute-name index",
            Self::Value(_) => "graphrecord-value index",
            Self::Bool(_) => "boolean index",
            Self::EndpointRole(_) => "edge-endpoint-role index",
            Self::FailureKind(_) => "failure-kind index",
            Self::Expanded(_) => "expanded index",
        }
    }
}

macro_rules! implement_dynamic_index {
    ($index:ty, $node:ty, $edge:ty) => {
        impl $index {
            pub(crate) fn supports_value_ordering(&self) -> bool {
                match self {
                    Self::Positional(_)
                    | Self::Node(_)
                    | Self::Edge(_)
                    | Self::Attribute(_)
                    | Self::Value(_)
                    | Self::Bool(_) => true,
                    Self::EndpointRole(_) | Self::FailureKind(_) => false,
                    Self::Expanded(expanded) => {
                        expanded.parent.supports_value_ordering()
                            && expanded
                                .child
                                .as_ref()
                                .is_none_or(Self::supports_value_ordering)
                    }
                }
            }

            fn supports_index_sorting(&self) -> bool {
                match self {
                    Self::Positional(_)
                    | Self::Node(_)
                    | Self::Edge(_)
                    | Self::Attribute(_)
                    | Self::Value(_)
                    | Self::Bool(_) => true,
                    Self::EndpointRole(_) | Self::FailureKind(_) => false,
                    Self::Expanded(expanded) => {
                        expanded.parent.supports_index_sorting()
                            && expanded
                                .child
                                .as_ref()
                                .is_none_or(Self::supports_index_sorting)
                    }
                }
            }

            pub(crate) fn has_same_domain(&self, other: &Self) -> bool {
                match (self, other) {
                    (Self::Positional(_), Self::Positional(_))
                    | (Self::Node(_), Self::Node(_))
                    | (Self::Edge(_), Self::Edge(_))
                    | (Self::Attribute(_), Self::Attribute(_))
                    | (Self::Value(_), Self::Value(_))
                    | (Self::Bool(_), Self::Bool(_))
                    | (Self::EndpointRole(_), Self::EndpointRole(_))
                    | (Self::FailureKind(_), Self::FailureKind(_)) => true,
                    (Self::Expanded(first), Self::Expanded(second)) => {
                        first.parent.has_same_domain(&second.parent)
                            && match (&first.child, &second.child) {
                                (Some(first), Some(second)) => first.has_same_domain(second),
                                _ => true,
                            }
                    }
                    _ => false,
                }
            }
        }

        impl Display for $index {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                match self {
                    Self::Positional(position) => position.fmt(formatter),
                    Self::Node(node) => node.fmt(formatter),
                    Self::Edge(edge) => edge.fmt(formatter),
                    Self::Attribute(attribute) => attribute.fmt(formatter),
                    Self::Value(value) => value.fmt(formatter),
                    Self::Bool(value) => value.fmt(formatter),
                    Self::EndpointRole(role) => role.fmt(formatter),
                    Self::FailureKind(kind) => kind.fmt(formatter),
                    Self::Expanded(expanded) => {
                        let parent = &expanded.parent;
                        match &expanded.child {
                            None => write!(formatter, "source({parent})"),
                            Some(child) => write!(formatter, "child({parent}, {child})"),
                        }
                    }
                }
            }
        }

        impl PartialOrd for $index {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                if !self.supports_value_ordering() || !other.supports_value_ordering() {
                    return None;
                }

                match (self, other) {
                    (Self::Positional(first), Self::Positional(second)) => {
                        first.partial_cmp(second)
                    }
                    (Self::Node(first), Self::Node(second)) => first.partial_cmp(second),
                    (Self::Edge(first), Self::Edge(second)) => first.partial_cmp(second),
                    (Self::Attribute(first), Self::Attribute(second)) => first.partial_cmp(second),
                    (Self::Value(first), Self::Value(second)) => first.partial_cmp(second),
                    (Self::Bool(first), Self::Bool(second)) => first.partial_cmp(second),
                    (Self::Expanded(first), Self::Expanded(second)) => {
                        let parent_ordering = first.parent.partial_cmp(&second.parent)?;
                        if parent_ordering != Ordering::Equal {
                            return Some(parent_ordering);
                        }

                        match (&first.child, &second.child) {
                            (None, None) => Some(Ordering::Equal),
                            (None, Some(_)) => Some(Ordering::Less),
                            (Some(_), None) => Some(Ordering::Greater),
                            (Some(first), Some(second)) => first.partial_cmp(second),
                        }
                    }
                    _ => None,
                }
            }
        }

        impl EnsureSortable for $index {
            fn find_incomparable<'a>(
                values: impl Iterator<Item = &'a Self>,
            ) -> Option<(usize, usize)>
            where
                Self: 'a,
            {
                let values: Vec<_> = values.collect();
                let first = values.first()?;

                match first {
                    Self::Positional(_)
                        if values
                            .iter()
                            .all(|value| matches!(value, Self::Positional(_))) =>
                    {
                        Position::find_incomparable(values.into_iter().map(|value| {
                            let Self::Positional(value) = value else {
                                unreachable!("dynamic indices were checked as positional indices")
                            };
                            value
                        }))
                    }
                    Self::Node(_) if values.iter().all(|value| matches!(value, Self::Node(_))) => {
                        <$node>::find_incomparable(values.into_iter().map(|value| {
                            let Self::Node(value) = value else {
                                unreachable!("dynamic indices were checked as node indices")
                            };
                            value
                        }))
                    }
                    Self::Edge(_) if values.iter().all(|value| matches!(value, Self::Edge(_))) => {
                        <$edge>::find_incomparable(values.into_iter().map(|value| {
                            let Self::Edge(value) = value else {
                                unreachable!("dynamic indices were checked as edge indices")
                            };
                            value
                        }))
                    }
                    Self::Attribute(_)
                        if values
                            .iter()
                            .all(|value| matches!(value, Self::Attribute(_))) =>
                    {
                        GraphRecordAttribute::find_incomparable(values.into_iter().map(|value| {
                            let Self::Attribute(value) = value else {
                                unreachable!(
                                    "dynamic indices were checked as attribute-name indices"
                                )
                            };
                            value
                        }))
                    }
                    Self::Value(_)
                        if values.iter().all(|value| matches!(value, Self::Value(_))) =>
                    {
                        GraphRecordValue::find_incomparable(values.into_iter().map(|value| {
                            let Self::Value(value) = value else {
                                unreachable!("dynamic indices were checked as value indices")
                            };
                            value
                        }))
                    }
                    Self::Bool(_) if values.iter().all(|value| matches!(value, Self::Bool(_))) => {
                        bool::find_incomparable(values.into_iter().map(|value| {
                            let Self::Bool(value) = value else {
                                unreachable!("dynamic indices were checked as boolean indices")
                            };
                            value
                        }))
                    }
                    Self::Expanded(_)
                        if values
                            .iter()
                            .all(|value| matches!(value, Self::Expanded(_))) =>
                    {
                        let true = values.iter().all(|value| value.supports_index_sorting()) else {
                            panic!(
                                "registry admitted EnsureSortable for an expanded dynamic index containing an unsortable domain"
                            );
                        };
                        let true = values.iter().all(|value| first.has_same_domain(value)) else {
                            panic!(
                                "registry admitted EnsureSortable for mixed expanded dynamic index domains"
                            );
                        };
                        incomparable_pair(values.into_iter())
                    }
                    Self::EndpointRole(_) | Self::FailureKind(_) => {
                        panic!(
                            "registry admitted EnsureSortable for an unsortable dynamic index domain"
                        )
                    }
                    _ => panic!("registry admitted EnsureSortable for mixed dynamic index domains"),
                }
            }
        }
    };
}

implement_dynamic_index!(DynIndexOwned, NodeIndex, EdgeIndex);
implement_dynamic_index!(DynIndexRef<'_>, &NodeIndex, &EdgeIndex);

impl IndexDomain for DynIndex {
    type Index<'a> = DynIndexRef<'a>;
    type Owned = DynIndexOwned;

    fn to_owned(index: &Self::Index<'_>) -> Self::Owned {
        match index {
            DynIndexRef::Positional(position) => DynIndexOwned::Positional(*position),
            DynIndexRef::Node(node) => DynIndexOwned::Node((*node).clone()),
            DynIndexRef::Edge(edge) => DynIndexOwned::Edge(**edge),
            DynIndexRef::Attribute(attribute) => DynIndexOwned::Attribute(attribute.clone()),
            DynIndexRef::Value(value) => DynIndexOwned::Value(value.clone()),
            DynIndexRef::Bool(value) => DynIndexOwned::Bool(*value),
            DynIndexRef::EndpointRole(role) => DynIndexOwned::EndpointRole(*role),
            DynIndexRef::FailureKind(kind) => DynIndexOwned::FailureKind(*kind),
            DynIndexRef::Expanded(expanded) => {
                let parent = <Self as IndexDomain>::to_owned(expanded.parent_index());
                let expanded = match expanded.child_index() {
                    None => DynExpandedOwned::source(parent),
                    Some(child) => {
                        DynExpandedOwned::child(parent, <Self as IndexDomain>::to_owned(child))
                    }
                };
                DynIndexOwned::Expanded(Box::new(expanded))
            }
        }
    }

    fn from_owned(owned: &Self::Owned) -> Self::Index<'_> {
        match owned {
            DynIndexOwned::Positional(position) => DynIndexRef::Positional(*position),
            DynIndexOwned::Node(node) => DynIndexRef::Node(node),
            DynIndexOwned::Edge(edge) => DynIndexRef::Edge(edge),
            DynIndexOwned::Attribute(attribute) => DynIndexRef::Attribute(attribute.clone()),
            DynIndexOwned::Value(value) => DynIndexRef::Value(value.clone()),
            DynIndexOwned::Bool(value) => DynIndexRef::Bool(*value),
            DynIndexOwned::EndpointRole(role) => DynIndexRef::EndpointRole(*role),
            DynIndexOwned::FailureKind(kind) => DynIndexRef::FailureKind(*kind),
            DynIndexOwned::Expanded(expanded) => {
                let parent = Self::from_owned(expanded.parent_index());
                let expanded = match expanded.child_index() {
                    None => DynExpandedRef::source(parent),
                    Some(child) => DynExpandedRef::child(parent, Self::from_owned(child)),
                };
                DynIndexRef::Expanded(Box::new(expanded))
            }
        }
    }
}

impl GroupKey for DynIndex {
    fn resolve_key<'a>(
        label: &'static str,
        graphrecord: &'a GraphRecord,
        key: &Self::Owned,
    ) -> QueryResult<Self::Index<'a>> {
        match key {
            DynIndexOwned::Positional(position) => {
                Positional::resolve_key(label, graphrecord, position).map(DynIndexRef::Positional)
            }
            DynIndexOwned::Node(node) => NodeIndex::resolve_key(label, graphrecord, node)
                .map(DynIndexRef::Node)
                .map_err(|failure| failure.at::<Self>(&Self::from_owned(key))),
            DynIndexOwned::Edge(edge) => EdgeIndex::resolve_key(label, graphrecord, edge)
                .map(DynIndexRef::Edge)
                .map_err(|failure| failure.at::<Self>(&Self::from_owned(key))),
            DynIndexOwned::Attribute(attribute) => {
                AttributeName::resolve_key(label, graphrecord, attribute)
                    .map(DynIndexRef::Attribute)
            }
            DynIndexOwned::Value(value) => {
                GraphRecordValue::resolve_key(label, graphrecord, value).map(DynIndexRef::Value)
            }
            DynIndexOwned::Bool(value) => {
                bool::resolve_key(label, graphrecord, value).map(DynIndexRef::Bool)
            }
            DynIndexOwned::EndpointRole(role) => {
                EdgeEndpointRole::resolve_key(label, graphrecord, role)
                    .map(DynIndexRef::EndpointRole)
            }
            DynIndexOwned::FailureKind(kind) => {
                FailureKind::resolve_key(label, graphrecord, kind).map(DynIndexRef::FailureKind)
            }
            DynIndexOwned::Expanded(expanded) => {
                let parent = Self::resolve_key(label, graphrecord, &expanded.parent)?;
                let child = expanded
                    .child
                    .as_ref()
                    .map(|child| Self::resolve_key(label, graphrecord, child))
                    .transpose()?;

                let expanded = match child {
                    None => DynExpandedRef::source(parent),
                    Some(child) => DynExpandedRef::child(parent, child),
                };

                Ok(DynIndexRef::Expanded(Box::new(expanded)))
            }
        }
    }
}
