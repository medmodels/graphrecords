use super::{
    capability::{CapabilityIdentifier, CapabilityRegistry},
    descriptor::{
        ArgumentDescriptor, ArgumentMissingPolicy, ArgumentValueSource, ArityDescriptor,
        DomainDescriptor, ExpressionDescriptor, IndexDescriptor, LaneShapeDescriptor,
        OrderDescriptor, RetentionDescriptor, ValueArgumentDescriptor, ValueDescriptor, ValueRole,
    },
};
use graphrecords_utils::aliases::GrHashMap;

pub type VariableIdentifier = usize;

#[derive(Clone, Debug)]
pub enum IndexPattern {
    Any,
    Registered,
    Capable(CapabilitySet),
    Entity,
    Concrete(DomainDescriptor),
    Expanded { parent: Box<Self>, child: Box<Self> },
    Variable(VariableIdentifier, Box<Self>),
}

#[derive(Clone, Debug)]
pub enum ValuePattern {
    Registered,
    Concrete(ValueDescriptor),
    Capable(CapabilitySet),
    GroupKeyIs(Box<IndexPattern>),
    IndexValue(IndexPattern),
    EntityReference(IndexPattern),
    Variable(VariableIdentifier, Box<Self>),
}

#[derive(Clone, Debug)]
pub struct CapabilitySet(Vec<CapabilityIdentifier>);

impl CapabilitySet {
    #[must_use]
    pub const fn new(capabilities: Vec<CapabilityIdentifier>) -> Self {
        Self(capabilities)
    }
}

#[derive(Clone, Debug)]
pub enum ShapePattern {
    Any,
    Indexed {
        index: IndexPattern,
        value: ValuePattern,
    },
    Bare {
        value: ValuePattern,
    },
    Variable(VariableIdentifier, Box<Self>),
}

#[derive(Clone, Debug)]
pub enum OrderPattern {
    Any,
    Ordered,
    Unordered,
    Variable(VariableIdentifier, Box<Self>),
}

#[derive(Clone, Debug)]
pub enum ArityPattern {
    Any,
    Multiple(OrderPattern),
    Single,
    Definite,
    Variable(VariableIdentifier, Box<Self>),
}

#[derive(Clone, Debug)]
pub enum ExpressionPattern {
    Lane {
        shape: ShapePattern,
        arity: ArityPattern,
    },
    Group {
        member: IndexPattern,
        key: IndexPattern,
        payload: Box<Self>,
    },
    Variable(VariableIdentifier, Box<Self>),
}

#[derive(Clone, Debug)]
pub enum AlignmentDescriptor {
    Keyed(IndexPattern),
    Unaligned,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum RetentionPattern {
    Any,
    Fixed(RetentionDescriptor),
}

#[derive(Clone, Debug)]
pub enum ArgumentPattern {
    Value {
        value: ValuePattern,
        alignment: AlignmentDescriptor,
        retention: RetentionPattern,
    },
    Set(ValuePattern),
    Field(DomainDescriptor),
    Selector(DomainDescriptor),
    Expression(ExpressionPattern),
}

#[derive(Clone, Debug, Default)]
pub struct Bindings {
    indices: GrHashMap<VariableIdentifier, IndexDescriptor>,
    values: GrHashMap<VariableIdentifier, ValueDescriptor>,
    shapes: GrHashMap<VariableIdentifier, LaneShapeDescriptor>,
    orders: GrHashMap<VariableIdentifier, OrderDescriptor>,
    arities: GrHashMap<VariableIdentifier, ArityDescriptor>,
    expressions: GrHashMap<VariableIdentifier, ExpressionDescriptor>,
    argument_retention: RetentionDescriptor,
}

impl Bindings {
    fn bind_index(&mut self, variable: VariableIdentifier, index: &IndexDescriptor) -> bool {
        if let Some(bound) = self.indices.get(&variable) {
            return bound == index;
        }

        self.indices.insert(variable, index.clone());
        true
    }

    #[must_use]
    pub fn index(&self, variable: VariableIdentifier) -> Option<&IndexDescriptor> {
        self.indices.get(&variable)
    }

    fn bind_value(&mut self, variable: VariableIdentifier, value: &ValueDescriptor) -> bool {
        if let Some(bound) = self.values.get(&variable) {
            return bound == value;
        }

        self.values.insert(variable, value.clone());
        true
    }

    #[must_use]
    pub fn value(&self, variable: VariableIdentifier) -> Option<&ValueDescriptor> {
        self.values.get(&variable)
    }

    fn bind_shape(&mut self, variable: VariableIdentifier, shape: &LaneShapeDescriptor) -> bool {
        if let Some(bound) = self.shapes.get(&variable) {
            return bound == shape;
        }

        self.shapes.insert(variable, shape.clone());
        true
    }

    #[must_use]
    pub fn shape(&self, variable: VariableIdentifier) -> Option<&LaneShapeDescriptor> {
        self.shapes.get(&variable)
    }

    fn bind_order(&mut self, variable: VariableIdentifier, order: OrderDescriptor) -> bool {
        if let Some(bound) = self.orders.get(&variable) {
            return *bound == order;
        }

        self.orders.insert(variable, order);
        true
    }

    #[must_use]
    pub fn order(&self, variable: VariableIdentifier) -> Option<OrderDescriptor> {
        self.orders.get(&variable).copied()
    }

    fn bind_arity(&mut self, variable: VariableIdentifier, arity: ArityDescriptor) -> bool {
        if let Some(bound) = self.arities.get(&variable) {
            return *bound == arity;
        }

        self.arities.insert(variable, arity);
        true
    }

    #[must_use]
    pub fn arity(&self, variable: VariableIdentifier) -> Option<ArityDescriptor> {
        self.arities.get(&variable).copied()
    }

    fn bind_expression(
        &mut self,
        variable: VariableIdentifier,
        expression: &ExpressionDescriptor,
    ) -> bool {
        if let Some(bound) = self.expressions.get(&variable) {
            return bound == expression;
        }

        self.expressions.insert(variable, expression.clone());
        true
    }

    #[must_use]
    pub fn expression(&self, variable: VariableIdentifier) -> Option<&ExpressionDescriptor> {
        self.expressions.get(&variable)
    }

    fn compose_retention(&mut self, retention: RetentionDescriptor) {
        if retention == RetentionDescriptor::Dropping {
            self.argument_retention = RetentionDescriptor::Dropping;
        }
    }

    #[must_use]
    pub const fn argument_retention(&self) -> RetentionDescriptor {
        self.argument_retention
    }
}

impl IndexPattern {
    fn matches(
        &self,
        index: &IndexDescriptor,
        capabilities: &CapabilityRegistry,
        bindings: &mut Bindings,
    ) -> bool {
        match self {
            Self::Any => true,
            Self::Registered => capabilities.contains_index(index),
            Self::Capable(required_capabilities) => required_capabilities
                .0
                .iter()
                .all(|capability| capabilities.index_has(*capability, index)),
            Self::Entity => capabilities.index_has(CapabilityIdentifier::Entity, index),
            Self::Concrete(domain) => {
                matches!(index, IndexDescriptor::Domain(candidate) if candidate == domain)
            }
            Self::Expanded { parent, child } => match index {
                IndexDescriptor::Expanded {
                    parent: parent_descriptor,
                    child: child_descriptor,
                } => {
                    parent.matches(parent_descriptor, capabilities, bindings)
                        && child.matches(child_descriptor, capabilities, bindings)
                }
                IndexDescriptor::Domain(_) | IndexDescriptor::ExpandedParent { .. } => false,
            },
            Self::Variable(variable, bound) => {
                if !bound.matches(index, capabilities, bindings) {
                    return false;
                }
                bindings.bind_index(*variable, index)
            }
        }
    }
}

impl ValuePattern {
    fn matches(
        &self,
        value: &ValueDescriptor,
        capabilities: &CapabilityRegistry,
        bindings: &mut Bindings,
    ) -> bool {
        match self {
            Self::Registered => capabilities.contains_value(value),
            Self::Concrete(descriptor) => value == descriptor,
            Self::Capable(required_capabilities) => required_capabilities
                .0
                .iter()
                .all(|capability| capabilities.value_has(*capability, value)),
            Self::GroupKeyIs(key_pattern) => capabilities
                .group_key(value)
                .is_some_and(|key| key_pattern.matches(&key, capabilities, bindings)),
            Self::IndexValue(index_pattern) => match value.role() {
                ValueRole::Index(index) => index_pattern.matches(index, capabilities, bindings),
                _ => false,
            },
            Self::EntityReference(index_pattern) => match value.role() {
                ValueRole::EntityReference(index) => {
                    index_pattern.matches(index, capabilities, bindings)
                }
                _ => false,
            },
            Self::Variable(variable, bound) => {
                bound.matches(value, capabilities, bindings)
                    && bindings.bind_value(*variable, value)
            }
        }
    }
}

impl ShapePattern {
    fn matches(
        &self,
        shape: &LaneShapeDescriptor,
        capabilities: &CapabilityRegistry,
        bindings: &mut Bindings,
    ) -> bool {
        match self {
            Self::Any => match shape {
                LaneShapeDescriptor::Indexed { .. } => true,
                LaneShapeDescriptor::Bare { value } => {
                    capabilities.value_has(CapabilityIdentifier::BareValue, value)
                }
            },
            Self::Indexed { index, value } => match shape {
                LaneShapeDescriptor::Indexed {
                    index: index_descriptor,
                    value: value_descriptor,
                } => {
                    index.matches(index_descriptor, capabilities, bindings)
                        && value.matches(value_descriptor, capabilities, bindings)
                }
                LaneShapeDescriptor::Bare { .. } => false,
            },
            Self::Bare { value } => match shape {
                LaneShapeDescriptor::Bare {
                    value: value_descriptor,
                } => value.matches(value_descriptor, capabilities, bindings),
                LaneShapeDescriptor::Indexed { .. } => false,
            },
            Self::Variable(variable, bound) => {
                bound.matches(shape, capabilities, bindings)
                    && bindings.bind_shape(*variable, shape)
            }
        }
    }
}

impl OrderPattern {
    fn matches(&self, order: OrderDescriptor, bindings: &mut Bindings) -> bool {
        match self {
            Self::Any => true,
            Self::Ordered => order == OrderDescriptor::Ordered,
            Self::Unordered => order == OrderDescriptor::Unordered,
            Self::Variable(variable, bound) => {
                bound.matches(order, bindings) && bindings.bind_order(*variable, order)
            }
        }
    }
}

impl ArityPattern {
    fn matches(&self, arity: ArityDescriptor, bindings: &mut Bindings) -> bool {
        match (self, arity) {
            (
                Self::Multiple(order),
                ArityDescriptor::Multiple {
                    order: order_descriptor,
                },
            ) => order.matches(order_descriptor, bindings),
            (Self::Any, _)
            | (Self::Single, ArityDescriptor::Single)
            | (Self::Definite, ArityDescriptor::Definite) => true,
            (Self::Variable(variable, bound), _) => {
                bound.matches(arity, bindings) && bindings.bind_arity(*variable, arity)
            }
            _ => false,
        }
    }
}

impl ExpressionPattern {
    #[must_use]
    pub fn matches(
        &self,
        expression: &ExpressionDescriptor,
        capabilities: &CapabilityRegistry,
    ) -> Option<Bindings> {
        let mut bindings = Bindings::default();
        self.matches_into(expression, capabilities, &mut bindings)
            .then_some(bindings)
    }

    fn matches_into(
        &self,
        expression: &ExpressionDescriptor,
        capabilities: &CapabilityRegistry,
        bindings: &mut Bindings,
    ) -> bool {
        if let Self::Variable(variable, bound) = self {
            return bound.matches_into(expression, capabilities, bindings)
                && bindings.bind_expression(*variable, expression);
        }

        match (self, expression) {
            (
                Self::Lane { shape, arity },
                ExpressionDescriptor::Lane {
                    shape: shape_descriptor,
                    arity: arity_descriptor,
                },
            ) => {
                shape.matches(shape_descriptor, capabilities, bindings)
                    && arity.matches(*arity_descriptor, bindings)
            }
            (
                Self::Group {
                    member,
                    key,
                    payload,
                },
                ExpressionDescriptor::Group {
                    member: member_descriptor,
                    key: key_descriptor,
                    payload: payload_descriptor,
                },
            ) => {
                member.matches(member_descriptor, capabilities, bindings)
                    && key.matches(key_descriptor, capabilities, bindings)
                    && payload.matches_into(payload_descriptor, capabilities, bindings)
            }
            _ => false,
        }
    }
}

impl AlignmentDescriptor {
    fn admits(
        &self,
        argument: &ValueArgumentDescriptor,
        capabilities: &CapabilityRegistry,
        bindings: &mut Bindings,
    ) -> bool {
        if matches!(argument.value().role(), ValueRole::Unit) {
            return false;
        }

        match argument.missing() {
            ArgumentMissingPolicy::None => {
                self.admits_source(argument.source(), capabilities, bindings)
            }
            ArgumentMissingPolicy::Drop => {
                self.admits_lookup(argument.source(), capabilities, bindings)
            }
            ArgumentMissingPolicy::Replace(replacement) => {
                self.admits_lookup(argument.source(), capabilities, bindings)
                    && self.admits_source(replacement, capabilities, bindings)
            }
        }
    }

    fn admits_source(
        &self,
        source: &ArgumentValueSource,
        capabilities: &CapabilityRegistry,
        bindings: &mut Bindings,
    ) -> bool {
        let ArgumentValueSource::Expression(expression) = source else {
            return true;
        };
        let ExpressionDescriptor::Lane { shape, arity } = expression else {
            return false;
        };

        match (self, shape, arity) {
            (
                Self::Keyed(pattern),
                LaneShapeDescriptor::Indexed { index, .. },
                ArityDescriptor::Multiple { .. },
            ) => pattern.matches(index, capabilities, bindings),
            (
                _,
                LaneShapeDescriptor::Bare { .. },
                ArityDescriptor::Single | ArityDescriptor::Definite,
            ) => true,
            _ => false,
        }
    }

    fn admits_lookup(
        &self,
        source: &ArgumentValueSource,
        capabilities: &CapabilityRegistry,
        bindings: &mut Bindings,
    ) -> bool {
        let ArgumentValueSource::Expression(ExpressionDescriptor::Lane { shape, arity }) = source
        else {
            return false;
        };

        match (self, shape, arity) {
            (
                Self::Keyed(pattern),
                LaneShapeDescriptor::Indexed { index, .. },
                ArityDescriptor::Multiple { .. },
            ) => pattern.matches(index, capabilities, bindings),
            (Self::Unaligned, LaneShapeDescriptor::Bare { .. }, ArityDescriptor::Single) => true,
            _ => false,
        }
    }
}

impl RetentionPattern {
    fn matches(self, retention: RetentionDescriptor) -> bool {
        match self {
            Self::Any => true,
            Self::Fixed(required) => required == retention,
        }
    }
}

impl ArgumentPattern {
    #[must_use]
    pub fn field<T: 'static>() -> Self {
        Self::Field(DomainDescriptor::of::<T>())
    }

    #[must_use]
    pub fn selector<T: 'static>() -> Self {
        Self::Selector(DomainDescriptor::of::<T>())
    }

    pub(super) fn matches(
        &self,
        argument: &ArgumentDescriptor,
        capabilities: &CapabilityRegistry,
        bindings: &mut Bindings,
    ) -> bool {
        match (self, argument) {
            (
                Self::Value {
                    value,
                    alignment,
                    retention,
                },
                ArgumentDescriptor::Value(descriptor),
            ) => {
                if !retention.matches(descriptor.retention())
                    || !alignment.admits(descriptor, capabilities, bindings)
                    || !Self::matches_value(value, descriptor, capabilities, bindings)
                {
                    return false;
                }

                bindings.compose_retention(descriptor.retention());

                true
            }
            (Self::Set(value), ArgumentDescriptor::Value(descriptor)) => {
                matches!(descriptor.missing(), ArgumentMissingPolicy::None)
                    && Self::admits_set_source(descriptor.source())
                    && value.matches(descriptor.value(), capabilities, bindings)
            }
            (Self::Field(pattern), ArgumentDescriptor::Field(field)) => pattern == field,
            (Self::Selector(pattern), ArgumentDescriptor::Selector(selector)) => {
                pattern == selector
            }
            (Self::Expression(pattern), ArgumentDescriptor::Expression(expression)) => {
                pattern.matches_into(expression, capabilities, bindings)
            }
            _ => false,
        }
    }

    const fn admits_set_source(source: &ArgumentValueSource) -> bool {
        match source {
            ArgumentValueSource::Literal(_) => true,
            ArgumentValueSource::Expression(expression) => {
                matches!(expression, ExpressionDescriptor::Lane { .. })
            }
        }
    }

    fn matches_value(
        pattern: &ValuePattern,
        argument: &ValueArgumentDescriptor,
        capabilities: &CapabilityRegistry,
        bindings: &mut Bindings,
    ) -> bool {
        if !pattern.matches(argument.value(), capabilities, bindings) {
            return false;
        }

        let ArgumentMissingPolicy::Replace(replacement) = argument.missing() else {
            return true;
        };

        pattern.matches(replacement.value(), capabilities, bindings)
    }
}
