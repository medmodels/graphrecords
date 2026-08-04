use super::{
    ArityDescriptor, Bindings, CapabilityRegistry, EmissionSpec, IndexDescriptor,
    LaneShapeDescriptor, OperandDescriptor, OrderDescriptor, OutArityTable, ValueDescriptor,
    VariableIdentifier,
};

#[derive(Clone, Debug)]
pub enum IndexDescriptorTemplate {
    Concrete(IndexDescriptor),
    Variable(VariableIdentifier),
    Expanded { parent: Box<Self>, child: Box<Self> },
    GroupKeyOf(VariableIdentifier),
}

impl IndexDescriptorTemplate {
    pub(super) fn fill(
        &self,
        bindings: &Bindings,
        capabilities: &CapabilityRegistry,
    ) -> IndexDescriptor {
        match self {
            Self::Concrete(index) => index.clone(),
            Self::Variable(variable) => bindings
                .index(*variable)
                .unwrap_or_else(|| {
                    panic!("registry output has no binding for index variable {variable}")
                })
                .clone(),
            Self::Expanded { parent, child } => IndexDescriptor::expanded(
                parent.fill(bindings, capabilities),
                child.fill(bindings, capabilities),
            ),
            Self::GroupKeyOf(variable) => {
                let value = bindings.value(*variable).unwrap_or_else(|| {
                    panic!("registry output has no binding for value variable {variable}")
                });

                capabilities.group_key(value).unwrap_or_else(|| {
                    panic!("registry admitted a grouping value without a group key domain")
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum ValueDescriptorTemplate {
    Concrete(ValueDescriptor),
    Variable(VariableIdentifier),
    Index(IndexDescriptorTemplate),
    EntityReference(IndexDescriptorTemplate),
}

impl ValueDescriptorTemplate {
    pub(super) fn fill(
        &self,
        bindings: &Bindings,
        capabilities: &CapabilityRegistry,
    ) -> ValueDescriptor {
        match self {
            Self::Concrete(value) => value.clone(),
            Self::Variable(variable) => bindings
                .value(*variable)
                .unwrap_or_else(|| {
                    panic!("registry output has no binding for value variable {variable}")
                })
                .clone(),
            Self::Index(index) => ValueDescriptor::index(index.fill(bindings, capabilities)),
            Self::EntityReference(index) => {
                ValueDescriptor::entity_reference_index(index.fill(bindings, capabilities))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum LaneShapeDescriptorTemplate {
    Indexed {
        index: IndexDescriptorTemplate,
        value: ValueDescriptorTemplate,
    },
    Bare {
        value: ValueDescriptorTemplate,
    },
    Variable(VariableIdentifier),
}

impl LaneShapeDescriptorTemplate {
    pub(super) fn fill(
        &self,
        bindings: &Bindings,
        capabilities: &CapabilityRegistry,
    ) -> LaneShapeDescriptor {
        match self {
            Self::Indexed { index, value } => LaneShapeDescriptor::Indexed {
                index: index.fill(bindings, capabilities),
                value: value.fill(bindings, capabilities),
            },
            Self::Bare { value } => LaneShapeDescriptor::Bare {
                value: value.fill(bindings, capabilities),
            },
            Self::Variable(variable) => bindings
                .shape(*variable)
                .unwrap_or_else(|| {
                    panic!("registry output has no binding for shape variable {variable}")
                })
                .clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum OrderDescriptorTemplate {
    Concrete(OrderDescriptor),
    Variable(VariableIdentifier),
}

impl OrderDescriptorTemplate {
    pub(super) fn fill(&self, bindings: &Bindings) -> OrderDescriptor {
        match self {
            Self::Concrete(order) => *order,
            Self::Variable(variable) => bindings.order(*variable).unwrap_or_else(|| {
                panic!("registry output has no binding for order variable {variable}")
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ArityDescriptorTemplate {
    Multiple(OrderDescriptorTemplate),
    Single,
    Definite,
    Variable(VariableIdentifier),
    EmissionOf {
        input: VariableIdentifier,
        emission: EmissionSpec,
    },
}

impl ArityDescriptorTemplate {
    pub(super) fn fill(&self, bindings: &Bindings, arities: &OutArityTable) -> ArityDescriptor {
        match self {
            Self::Multiple(order) => ArityDescriptor::Multiple {
                order: order.fill(bindings),
            },
            Self::Single => ArityDescriptor::Single,
            Self::Definite => ArityDescriptor::Definite,
            Self::Variable(variable) => Self::bound_arity(bindings, *variable),
            Self::EmissionOf { input, emission } => arities.resolve(
                *emission,
                Self::bound_arity(bindings, *input),
                bindings.argument_retention(),
            ),
        }
    }

    fn bound_arity(bindings: &Bindings, variable: VariableIdentifier) -> ArityDescriptor {
        bindings.arity(variable).unwrap_or_else(|| {
            panic!("registry output has no binding for arity variable {variable}")
        })
    }
}

#[derive(Clone, Debug)]
pub enum OperandDescriptorTemplate {
    Lane {
        shape: LaneShapeDescriptorTemplate,
        arity: ArityDescriptorTemplate,
    },
    Group {
        member: IndexDescriptorTemplate,
        key: IndexDescriptorTemplate,
        payload: Box<Self>,
    },
    Variable(VariableIdentifier),
}

impl OperandDescriptorTemplate {
    pub(super) fn fill(
        &self,
        bindings: &Bindings,
        capabilities: &CapabilityRegistry,
        arities: &OutArityTable,
    ) -> OperandDescriptor {
        match self {
            Self::Lane { shape, arity } => OperandDescriptor::Lane {
                shape: shape.fill(bindings, capabilities),
                arity: arity.fill(bindings, arities),
            },
            Self::Group {
                member,
                key,
                payload,
            } => OperandDescriptor::Group {
                member: member.fill(bindings, capabilities),
                key: key.fill(bindings, capabilities),
                payload: Box::new(payload.fill(bindings, capabilities, arities)),
            },
            Self::Variable(variable) => bindings
                .operand(*variable)
                .unwrap_or_else(|| {
                    panic!("registry output has no binding for operand variable {variable}")
                })
                .clone(),
        }
    }
}
