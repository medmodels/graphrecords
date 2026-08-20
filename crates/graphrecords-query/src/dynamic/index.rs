use crate::{
    EdgeEndpointRole, FailureKind, IndexDomain, Position, Positional, QueryResult,
    capabilities::{EnsureSortable, incomparable_pair},
    operations::IndexTiebreak,
    registry::IndexDescriptor,
};
use graphrecords_core::{
    GraphRecord,
    graphrecord::{
        AttributeName, AttributeNameView, EdgeAddress, EdgeIndex, Group, GroupAddress, GroupView,
        NodeAddress, NodeIndex, NodeIndexView, Value,
    },
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
    Group(Group),
    Attribute(AttributeName),
    Value(Value),
    Bool(bool),
    EndpointRole(EdgeEndpointRole),
    FailureKind(FailureKind),
    Expanded(Box<DynExpandedOwned>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DynIndexView<'a> {
    Positional(Position),
    Node(NodeIndexView<'a>),
    Edge(EdgeIndex),
    Group(GroupView<'a>),
    Attribute(AttributeNameView<'a>),
    Value(Value),
    Bool(bool),
    EndpointRole(EdgeEndpointRole),
    FailureKind(FailureKind),
    Expanded(Box<DynExpandedView<'a>>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum DynIndexAddress {
    Positional(Position),
    Node(NodeAddress),
    Edge(EdgeAddress),
    Group(GroupAddress),
    Attribute(AttributeName),
    Value(Value),
    Bool(bool),
    EndpointRole(EdgeEndpointRole),
    FailureKind(FailureKind),
    Expanded(Box<DynExpandedAddress>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DynExpandedOwned {
    parent: DynIndexOwned,
    child: Option<DynIndexOwned>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DynExpandedView<'a> {
    parent: DynIndexView<'a>,
    child: Option<DynIndexView<'a>>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DynExpandedAddress {
    parent: DynIndexAddress,
    child: Option<DynIndexAddress>,
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
    pub const fn is_parent(&self) -> bool {
        self.child.is_none()
    }

    #[must_use]
    pub const fn parent(parent: DynIndexOwned) -> Self {
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

impl<'a> DynExpandedView<'a> {
    #[must_use]
    pub const fn parent_index(&self) -> &DynIndexView<'a> {
        &self.parent
    }

    #[must_use]
    pub const fn child_index(&self) -> Option<&DynIndexView<'a>> {
        self.child.as_ref()
    }

    #[must_use]
    pub fn into_parts(self) -> (DynIndexView<'a>, Option<DynIndexView<'a>>) {
        (self.parent, self.child)
    }

    #[must_use]
    pub const fn is_parent(&self) -> bool {
        self.child.is_none()
    }

    #[must_use]
    pub const fn parent(parent: DynIndexView<'a>) -> Self {
        Self {
            parent,
            child: None,
        }
    }

    #[must_use]
    pub const fn child(parent: DynIndexView<'a>, child: DynIndexView<'a>) -> Self {
        Self {
            parent,
            child: Some(child),
        }
    }
}

impl DynExpandedAddress {
    #[must_use]
    pub const fn parent_index(&self) -> &DynIndexAddress {
        &self.parent
    }

    #[must_use]
    pub const fn child_index(&self) -> Option<&DynIndexAddress> {
        self.child.as_ref()
    }

    #[must_use]
    pub fn into_parts(self) -> (DynIndexAddress, Option<DynIndexAddress>) {
        (self.parent, self.child)
    }

    #[must_use]
    pub const fn is_parent(&self) -> bool {
        self.child.is_none()
    }

    #[must_use]
    pub const fn parent(parent: DynIndexAddress) -> Self {
        Self {
            parent,
            child: None,
        }
    }

    #[must_use]
    pub const fn child(parent: DynIndexAddress, child: DynIndexAddress) -> Self {
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
            Self::Group(_) => IndexDescriptor::domain::<Group>(),
            Self::Attribute(_) => IndexDescriptor::domain::<AttributeName>(),
            Self::Value(_) => IndexDescriptor::domain::<Value>(),
            Self::Bool(_) => IndexDescriptor::domain::<bool>(),
            Self::EndpointRole(_) => IndexDescriptor::domain::<EdgeEndpointRole>(),
            Self::FailureKind(_) => IndexDescriptor::domain::<FailureKind>(),
            Self::Expanded(expanded) => {
                let parent = expanded.parent_index().descriptor();

                expanded.child_index().map_or_else(
                    || IndexDescriptor::expanded_parent(parent.clone()),
                    |child| IndexDescriptor::expanded(parent.clone(), child.descriptor()),
                )
            }
        }
    }
}

impl DynIndexView<'_> {
    pub(crate) const fn description(&self) -> &'static str {
        match self {
            Self::Positional(_) => "positional index",
            Self::Node(_) => "node index",
            Self::Edge(_) => "edge index",
            Self::Group(_) => "group index",
            Self::Attribute(_) => "attribute-name index",
            Self::Value(_) => "value index",
            Self::Bool(_) => "boolean index",
            Self::EndpointRole(_) => "edge-endpoint-role index",
            Self::FailureKind(_) => "failure-kind index",
            Self::Expanded(_) => "expanded index",
        }
    }
}

macro_rules! implement_dynamic_index {
    ($index:ty, $node:ty, $group:ty, $attribute:ty) => {
        impl $index {
            pub(crate) fn supports_value_ordering(&self) -> bool {
                match self {
                    Self::Positional(_)
                    | Self::Node(_)
                    | Self::Group(_)
                    | Self::Attribute(_)
                    | Self::Value(_)
                    | Self::Bool(_) => true,
                    Self::Edge(_) | Self::EndpointRole(_) | Self::FailureKind(_) => false,
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
                    | Self::Group(_)
                    | Self::Attribute(_)
                    | Self::Value(_)
                    | Self::Bool(_) => true,
                    Self::Edge(_) | Self::EndpointRole(_) | Self::FailureKind(_) => false,
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
                    | (Self::Group(_), Self::Group(_))
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
                    Self::Group(group) => group.fmt(formatter),
                    Self::Attribute(attribute) => attribute.fmt(formatter),
                    Self::Value(value) => value.fmt(formatter),
                    Self::Bool(value) => value.fmt(formatter),
                    Self::EndpointRole(role) => role.fmt(formatter),
                    Self::FailureKind(kind) => kind.fmt(formatter),
                    Self::Expanded(expanded) => {
                        let parent = &expanded.parent;
                        match &expanded.child {
                            None => write!(formatter, "parent({parent})"),
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
                    (Self::Group(first), Self::Group(second)) => first.partial_cmp(second),
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
                    Self::Group(_) if values.iter().all(|value| matches!(value, Self::Group(_))) => {
                        <$group>::find_incomparable(values.into_iter().map(|value| {
                            let Self::Group(value) = value else {
                                unreachable!("dynamic indices were checked as group indices")
                            };
                            value
                        }))
                    }
                    Self::Attribute(_)
                        if values
                            .iter()
                            .all(|value| matches!(value, Self::Attribute(_))) =>
                    {
                        <$attribute>::find_incomparable(values.into_iter().map(|value| {
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
                        Value::find_incomparable(values.into_iter().map(|value| {
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
                        if !values.iter().all(|value| value.supports_index_sorting()) {
                            panic!(
                                "registry admitted EnsureSortable for an expanded dynamic index containing an unsortable domain"
                            );
                        }
                        if !values.iter().all(|value| first.has_same_domain(value)) {
                            panic!(
                                "registry admitted EnsureSortable for mixed expanded dynamic index domains"
                            );
                        }
                        incomparable_pair(values.into_iter())
                    }
                    Self::Edge(_) | Self::EndpointRole(_) | Self::FailureKind(_) => {
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

implement_dynamic_index!(DynIndexOwned, NodeIndex, Group, AttributeName);
implement_dynamic_index!(
    DynIndexView<'_>,
    NodeIndexView<'_>,
    GroupView<'_>,
    AttributeNameView<'_>
);

impl DynIndex {
    fn tiebreak_expanded<T, F: Fn(&T) -> &DynIndexAddress>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    ) {
        let identities: Vec<_> = run
            .iter()
            .map(|element| Self::index(graphrecord, address(element)))
            .collect();

        let sortable = identities.first().is_some_and(|first| {
            first.supports_index_sorting()
                && identities
                    .iter()
                    .all(|identity| first.has_same_domain(identity))
        });

        if !sortable || EnsureSortable::find_incomparable(identities.iter()).is_some() {
            return;
        }

        run.sort_by(|left, right| {
            Self::index(graphrecord, address(left))
                .partial_cmp(&Self::index(graphrecord, address(right)))
                .unwrap_or_else(|| {
                    panic!("EnsureSortable admitted an incomparable pair of identities")
                })
        });
    }
}

impl IndexDomain for DynIndex {
    type Address = DynIndexAddress;
    type Index<'a> = DynIndexView<'a>;
    type Owned = DynIndexOwned;

    fn index<'a>(graphrecord: &'a GraphRecord, address: &Self::Address) -> Self::Index<'a> {
        match address {
            DynIndexAddress::Positional(position) => {
                DynIndexView::Positional(Positional::index(graphrecord, position))
            }
            DynIndexAddress::Node(node) => DynIndexView::Node(NodeIndex::index(graphrecord, node)),
            DynIndexAddress::Edge(edge) => DynIndexView::Edge(EdgeIndex::index(graphrecord, edge)),
            DynIndexAddress::Group(group) => DynIndexView::Group(Group::index(graphrecord, group)),
            DynIndexAddress::Attribute(attribute) => {
                DynIndexView::Attribute(AttributeName::index(graphrecord, attribute))
            }
            DynIndexAddress::Value(value) => DynIndexView::Value(Value::index(graphrecord, value)),
            DynIndexAddress::Bool(value) => DynIndexView::Bool(bool::index(graphrecord, value)),
            DynIndexAddress::EndpointRole(role) => {
                DynIndexView::EndpointRole(EdgeEndpointRole::index(graphrecord, role))
            }
            DynIndexAddress::FailureKind(kind) => {
                DynIndexView::FailureKind(FailureKind::index(graphrecord, kind))
            }
            DynIndexAddress::Expanded(expanded) => {
                let parent = Self::index(graphrecord, expanded.parent_index());
                let expanded = match expanded.child_index() {
                    None => DynExpandedView::parent(parent),
                    Some(child) => DynExpandedView::child(parent, Self::index(graphrecord, child)),
                };

                DynIndexView::Expanded(Box::new(expanded))
            }
        }
    }

    fn own_index(index: &Self::Index<'_>) -> Self::Owned {
        match index {
            DynIndexView::Positional(position) => {
                DynIndexOwned::Positional(Positional::own_index(position))
            }
            DynIndexView::Node(node) => DynIndexOwned::Node(NodeIndex::own_index(node)),
            DynIndexView::Edge(edge) => DynIndexOwned::Edge(EdgeIndex::own_index(edge)),
            DynIndexView::Group(group) => DynIndexOwned::Group(Group::own_index(group)),
            DynIndexView::Attribute(attribute) => {
                DynIndexOwned::Attribute(AttributeName::own_index(attribute))
            }
            DynIndexView::Value(value) => DynIndexOwned::Value(Value::own_index(value)),
            DynIndexView::Bool(value) => DynIndexOwned::Bool(bool::own_index(value)),
            DynIndexView::EndpointRole(role) => {
                DynIndexOwned::EndpointRole(EdgeEndpointRole::own_index(role))
            }
            DynIndexView::FailureKind(kind) => {
                DynIndexOwned::FailureKind(FailureKind::own_index(kind))
            }
            DynIndexView::Expanded(expanded) => {
                let parent = Self::own_index(expanded.parent_index());
                let expanded = match expanded.child_index() {
                    None => DynExpandedOwned::parent(parent),
                    Some(child) => DynExpandedOwned::child(parent, Self::own_index(child)),
                };

                DynIndexOwned::Expanded(Box::new(expanded))
            }
        }
    }

    fn borrow_index(owned: &Self::Owned) -> Self::Index<'_> {
        match owned {
            DynIndexOwned::Positional(position) => {
                DynIndexView::Positional(Positional::borrow_index(position))
            }
            DynIndexOwned::Node(node) => DynIndexView::Node(NodeIndex::borrow_index(node)),
            DynIndexOwned::Edge(edge) => DynIndexView::Edge(EdgeIndex::borrow_index(edge)),
            DynIndexOwned::Group(group) => DynIndexView::Group(Group::borrow_index(group)),
            DynIndexOwned::Attribute(attribute) => {
                DynIndexView::Attribute(AttributeName::borrow_index(attribute))
            }
            DynIndexOwned::Value(value) => DynIndexView::Value(Value::borrow_index(value)),
            DynIndexOwned::Bool(value) => DynIndexView::Bool(bool::borrow_index(value)),
            DynIndexOwned::EndpointRole(role) => {
                DynIndexView::EndpointRole(EdgeEndpointRole::borrow_index(role))
            }
            DynIndexOwned::FailureKind(kind) => {
                DynIndexView::FailureKind(FailureKind::borrow_index(kind))
            }
            DynIndexOwned::Expanded(expanded) => {
                let parent = Self::borrow_index(expanded.parent_index());
                let expanded = match expanded.child_index() {
                    None => DynExpandedView::parent(parent),
                    Some(child) => DynExpandedView::child(parent, Self::borrow_index(child)),
                };
                DynIndexView::Expanded(Box::new(expanded))
            }
        }
    }

    fn resolve(
        graphrecord: &GraphRecord,
        owned: &Self::Owned,
        label: &'static str,
    ) -> QueryResult<Self::Address> {
        match owned {
            DynIndexOwned::Positional(position) => {
                Positional::resolve(graphrecord, position, label).map(DynIndexAddress::Positional)
            }
            DynIndexOwned::Node(node) => {
                NodeIndex::resolve(graphrecord, node, label).map(DynIndexAddress::Node)
            }
            DynIndexOwned::Edge(edge) => {
                EdgeIndex::resolve(graphrecord, edge, label).map(DynIndexAddress::Edge)
            }
            DynIndexOwned::Group(group) => {
                Group::resolve(graphrecord, group, label).map(DynIndexAddress::Group)
            }
            DynIndexOwned::Attribute(attribute) => {
                AttributeName::resolve(graphrecord, attribute, label)
                    .map(DynIndexAddress::Attribute)
            }
            DynIndexOwned::Value(value) => {
                Value::resolve(graphrecord, value, label).map(DynIndexAddress::Value)
            }
            DynIndexOwned::Bool(value) => {
                bool::resolve(graphrecord, value, label).map(DynIndexAddress::Bool)
            }
            DynIndexOwned::EndpointRole(role) => {
                EdgeEndpointRole::resolve(graphrecord, role, label)
                    .map(DynIndexAddress::EndpointRole)
            }
            DynIndexOwned::FailureKind(kind) => {
                FailureKind::resolve(graphrecord, kind, label).map(DynIndexAddress::FailureKind)
            }
            DynIndexOwned::Expanded(expanded) => {
                let parent = Self::resolve(graphrecord, expanded.parent_index(), label)?;
                let expanded = match expanded.child_index() {
                    None => DynExpandedAddress::parent(parent),
                    Some(child) => {
                        DynExpandedAddress::child(parent, Self::resolve(graphrecord, child, label)?)
                    }
                };

                Ok(DynIndexAddress::Expanded(Box::new(expanded)))
            }
        }
    }
}

impl IndexTiebreak for DynIndex {
    fn tiebreak<T, F: Fn(&T) -> &Self::Address>(
        graphrecord: &GraphRecord,
        run: &mut [T],
        address: F,
    ) {
        if run
            .iter()
            .all(|element| matches!(address(element), DynIndexAddress::Positional(_)))
        {
            Positional::tiebreak(graphrecord, run, |element| match address(element) {
                DynIndexAddress::Positional(position) => position,
                _ => unreachable!("dynamic addresses were checked as positional addresses"),
            });

            return;
        }

        if run
            .iter()
            .all(|element| matches!(address(element), DynIndexAddress::Node(_)))
        {
            NodeIndex::tiebreak(graphrecord, run, |element| match address(element) {
                DynIndexAddress::Node(node) => node,
                _ => unreachable!("dynamic addresses were checked as node addresses"),
            });

            return;
        }

        if run
            .iter()
            .all(|element| matches!(address(element), DynIndexAddress::Group(_)))
        {
            Group::tiebreak(graphrecord, run, |element| match address(element) {
                DynIndexAddress::Group(group) => group,
                _ => unreachable!("dynamic addresses were checked as group addresses"),
            });

            return;
        }

        if run
            .iter()
            .all(|element| matches!(address(element), DynIndexAddress::Attribute(_)))
        {
            AttributeName::tiebreak(graphrecord, run, |element| match address(element) {
                DynIndexAddress::Attribute(attribute) => attribute,
                _ => unreachable!("dynamic addresses were checked as attribute-name addresses"),
            });

            return;
        }

        if run
            .iter()
            .all(|element| matches!(address(element), DynIndexAddress::Value(_)))
        {
            Value::tiebreak(graphrecord, run, |element| match address(element) {
                DynIndexAddress::Value(value) => value,
                _ => unreachable!("dynamic addresses were checked as value addresses"),
            });

            return;
        }

        if run
            .iter()
            .all(|element| matches!(address(element), DynIndexAddress::Bool(_)))
        {
            bool::tiebreak(graphrecord, run, |element| match address(element) {
                DynIndexAddress::Bool(value) => value,
                _ => unreachable!("dynamic addresses were checked as boolean addresses"),
            });

            return;
        }

        if run
            .iter()
            .all(|element| matches!(address(element), DynIndexAddress::Expanded(_)))
        {
            Self::tiebreak_expanded(graphrecord, run, address);
        }
    }
}
