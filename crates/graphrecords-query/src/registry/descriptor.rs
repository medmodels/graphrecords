use crate::{EntityIndexDomain, IndexDomain, Unit, ValueDomain};
use std::any::{TypeId, type_name};

struct IndexValueDescriptor;
struct EntityReferenceDescriptor;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct DomainDescriptor {
    identifier: TypeId,
    name: &'static str,
}

impl DomainDescriptor {
    #[must_use]
    pub fn of<T: 'static>() -> Self {
        Self {
            identifier: TypeId::of::<T>(),
            name: type_name::<T>(),
        }
    }

    #[must_use]
    pub fn is<T: 'static>(&self) -> bool {
        self.identifier == TypeId::of::<T>()
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum IndexDescriptor {
    Domain(DomainDescriptor),
    Expanded { parent: Box<Self>, child: Box<Self> },
    ExpandedParent { parent: Box<Self> },
}

impl IndexDescriptor {
    #[must_use]
    pub fn domain<I: IndexDomain>() -> Self {
        Self::Domain(DomainDescriptor::of::<I>())
    }

    #[must_use]
    pub fn expanded(parent: Self, child: Self) -> Self {
        Self::Expanded {
            parent: Box::new(parent),
            child: Box::new(child),
        }
    }

    #[must_use]
    pub fn expanded_parent(parent: Self) -> Self {
        Self::ExpandedParent {
            parent: Box::new(parent),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ValueRole {
    Value,
    Index(IndexDescriptor),
    EntityReference(IndexDescriptor),
    Unit,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ValueDescriptor {
    domain: DomainDescriptor,
    role: ValueRole,
}

impl ValueDescriptor {
    #[must_use]
    pub fn value<V: ValueDomain>() -> Self {
        Self {
            domain: DomainDescriptor::of::<V>(),
            role: ValueRole::Value,
        }
    }

    #[must_use]
    pub fn index(index: IndexDescriptor) -> Self {
        Self {
            domain: DomainDescriptor::of::<IndexValueDescriptor>(),
            role: ValueRole::Index(index),
        }
    }

    #[must_use]
    pub fn entity_reference<E: EntityIndexDomain>() -> Self {
        Self {
            domain: DomainDescriptor::of::<EntityReferenceDescriptor>(),
            role: ValueRole::EntityReference(IndexDescriptor::domain::<E>()),
        }
    }

    #[must_use]
    pub fn entity_reference_index(index: IndexDescriptor) -> Self {
        Self {
            domain: DomainDescriptor::of::<EntityReferenceDescriptor>(),
            role: ValueRole::EntityReference(index),
        }
    }

    #[must_use]
    pub fn unit() -> Self {
        Self {
            domain: DomainDescriptor::of::<Unit>(),
            role: ValueRole::Unit,
        }
    }

    #[must_use]
    pub const fn domain(&self) -> &DomainDescriptor {
        &self.domain
    }

    #[must_use]
    pub const fn role(&self) -> &ValueRole {
        &self.role
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum LaneShapeDescriptor {
    Indexed {
        index: IndexDescriptor,
        value: ValueDescriptor,
    },
    Bare {
        value: ValueDescriptor,
    },
}

impl LaneShapeDescriptor {
    #[must_use]
    pub const fn value(&self) -> &ValueDescriptor {
        match self {
            Self::Indexed { value, .. } | Self::Bare { value } => value,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum OrderDescriptor {
    Ordered,
    Unordered,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ArityDescriptor {
    Multiple { order: OrderDescriptor },
    Single,
    Definite,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ExpressionDescriptor {
    Lane {
        shape: LaneShapeDescriptor,
        arity: ArityDescriptor,
    },
    Group {
        member: IndexDescriptor,
        key: IndexDescriptor,
        payload: Box<Self>,
    },
}

impl ExpressionDescriptor {
    #[must_use]
    pub fn lane_shape(&self) -> &LaneShapeDescriptor {
        match self {
            Self::Lane { shape, .. } => shape,
            Self::Group { payload, .. } => payload.lane_shape(),
        }
    }

    #[must_use]
    pub fn lane_arity(&self) -> ArityDescriptor {
        match self {
            Self::Lane { arity, .. } => *arity,
            Self::Group { payload, .. } => payload.lane_arity(),
        }
    }

    #[must_use]
    pub fn group_depth(&self) -> usize {
        match self {
            Self::Lane { .. } => 0,
            Self::Group { payload, .. } => 1 + payload.group_depth(),
        }
    }

    #[must_use]
    pub fn with_lane_value(&self, value: ValueDescriptor) -> Self {
        match self {
            Self::Lane { shape, arity } => Self::Lane {
                shape: match shape {
                    LaneShapeDescriptor::Indexed { index, .. } => LaneShapeDescriptor::Indexed {
                        index: index.clone(),
                        value,
                    },
                    LaneShapeDescriptor::Bare { .. } => LaneShapeDescriptor::Bare { value },
                },
                arity: *arity,
            },
            Self::Group {
                member,
                key,
                payload,
            } => Self::Group {
                member: member.clone(),
                key: key.clone(),
                payload: Box::new(payload.with_lane_value(value)),
            },
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum RetentionDescriptor {
    #[default]
    Preserving,
    Dropping,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ArgumentValueSource {
    Literal(ValueDescriptor),
    Expression(ExpressionDescriptor),
}

impl ArgumentValueSource {
    #[must_use]
    pub fn value(&self) -> &ValueDescriptor {
        match self {
            Self::Literal(value) => value,
            Self::Expression(expression) => expression.lane_shape().value(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ArgumentMissingPolicy {
    None,
    Drop,
    Replace(Box<ValueArgumentDescriptor>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ValueArgumentDescriptor {
    source: ArgumentValueSource,
    missing: ArgumentMissingPolicy,
}

impl ValueArgumentDescriptor {
    #[must_use]
    pub const fn literal(value: ValueDescriptor) -> Self {
        Self {
            source: ArgumentValueSource::Literal(value),
            missing: ArgumentMissingPolicy::None,
        }
    }

    #[must_use]
    pub const fn expression(expression: ExpressionDescriptor) -> Self {
        Self {
            source: ArgumentValueSource::Expression(expression),
            missing: ArgumentMissingPolicy::None,
        }
    }

    #[must_use]
    pub fn with_missing(self, missing: ArgumentMissingPolicy) -> Self {
        Self {
            source: self.source,
            missing,
        }
    }

    #[must_use]
    pub const fn source(&self) -> &ArgumentValueSource {
        &self.source
    }

    #[must_use]
    pub const fn missing(&self) -> &ArgumentMissingPolicy {
        &self.missing
    }

    #[must_use]
    pub fn value(&self) -> &ValueDescriptor {
        self.source.value()
    }

    #[must_use]
    pub fn retention(&self) -> RetentionDescriptor {
        match &self.missing {
            ArgumentMissingPolicy::Drop => RetentionDescriptor::Dropping,
            ArgumentMissingPolicy::None => RetentionDescriptor::Preserving,
            ArgumentMissingPolicy::Replace(replacement) => replacement.retention(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ArgumentDescriptor {
    Value(ValueArgumentDescriptor),
    Field(DomainDescriptor),
    Selector(DomainDescriptor),
    Expression(ExpressionDescriptor),
}

impl ArgumentDescriptor {
    #[must_use]
    pub fn field<T: 'static>() -> Self {
        Self::Field(DomainDescriptor::of::<T>())
    }

    #[must_use]
    pub fn selector<T: 'static>() -> Self {
        Self::Selector(DomainDescriptor::of::<T>())
    }
}
