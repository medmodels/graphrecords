pub mod describe;
pub mod witness;

use super::{
    ArgumentDescriptor, ArgumentPattern, ArityDescriptorTemplate, ArityPattern, CapabilityRegistry,
    EmissionSpec, ExpressionDescriptor, ExpressionDescriptorTemplate, ExpressionPattern,
    OutArityTable,
};
#[cfg(feature = "dynamic")]
use crate::dynamic::DynApplier;
use crate::{
    IndexDomain,
    element::{Arity, ElementShape},
    expressions::{Expression, ExpressionHandle, GroupedExpression},
    operations::operation_manifests,
};
use describe::{DescribeArity, DescribeExpression, DescribeIndex, DescribeShape};
pub(crate) use graphrecords_macros::operation_manifest;
pub use witness::{
    AbsoluteCapability, AddCapability, ArgumentWitness, ArityWitness, BareValueCapability,
    CastBoolCapability, CastDateTimeCapability, CastDurationCapability, CastFloatCapability,
    CastIntCapability, CastStringCapability, CeilCapability, ClipCapability, CubeRootCapability,
    DivideCapability, ElementShapeWitness, EntityAttributesWitness, EntityWitness,
    EnumerableArityWitness, EqualityCapability, EquivalenceCapability, ExponentialCapability,
    FloorCapability, GroupKeyWitness, GroupMemberWitness, GroupMembershipWitness,
    GroupingCapability, IndexWitness, IntCapability, KindTestCapability, LogarithmCapability,
    MedianCapability, ModeCapability, ModuloCapability, MultiplyCapability, NegateCapability,
    OrderingCapability, PowerCapability, RoundCapability, ScalarCapability,
    ScalarKindTestCapability, SetSourceWitness, SignCapability, SortableCapability,
    SquareRootCapability, StringCapability, SubtractCapability, TransitionAttributeNameCapability,
    TransitionAttributeNameIndexCapability, TransitionBoolIndexCapability,
    TransitionFailureKindIndexCapability, TransitionFailureKindValueCapability,
    TransitionGroupIndexCapability, TransitionMaskCapability, TransitionNodeIndexCapability,
    TransitionPositionalIndexCapability, TransitionScalarCapability,
    TransitionValueIndexCapability, ValueDomainCapability, ValueDomainOnly, ValueWitness,
};

const ELEMENT_INPUT_ARITY: usize = 0;

pub struct OperationRegistry {
    capabilities: CapabilityRegistry,
    arities: OutArityTable,
    manifests: Vec<OperationManifest>,
}

impl OperationRegistry {
    #[must_use]
    pub fn builtins() -> Self {
        Self {
            capabilities: CapabilityRegistry::builtins(),
            arities: OutArityTable::builtins(),
            manifests: operation_manifests(),
        }
    }

    #[must_use]
    pub fn resolve(
        &self,
        method: &str,
        input: &ExpressionDescriptor,
    ) -> Option<ExpressionDescriptor> {
        self.resolve_with_arguments(method, input, &[])
    }

    #[must_use]
    pub fn resolve_with_arguments(
        &self,
        method: &str,
        input: &ExpressionDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<ExpressionDescriptor> {
        self.resolve_entry(method, input, arguments)
            .map(|resolved| resolved.0)
    }

    #[cfg(feature = "dynamic")]
    pub(crate) fn resolve_dispatch(
        &self,
        method: &str,
        input: &ExpressionDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<(ExpressionDescriptor, DynApplier)> {
        self.resolve_entry(method, input, arguments)
            .map(|(output, entry)| (output, entry.applier))
    }

    fn resolve_entry(
        &self,
        method: &str,
        input: &ExpressionDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<(ExpressionDescriptor, &OperationManifestEntry)> {
        if let Some(resolved) = self.resolve_here(method, input, arguments) {
            return Some(resolved);
        }

        let ExpressionDescriptor::Group {
            member,
            key,
            payload,
        } = input
        else {
            return None;
        };
        let (payload, entry) = self.resolve_entry(method, payload, arguments)?;
        let output = ExpressionDescriptor::Group {
            member: member.clone(),
            key: key.clone(),
            payload: Box::new(payload),
        };

        Some((output, entry))
    }

    fn resolve_here(
        &self,
        method: &str,
        input: &ExpressionDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<(ExpressionDescriptor, &OperationManifestEntry)> {
        self.manifests
            .iter()
            .filter(|manifest| manifest.method() == method)
            .find_map(|manifest| {
                manifest.resolve(&self.capabilities, &self.arities, input, arguments)
            })
    }

    pub fn method_names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.manifests.iter().map(OperationManifest::method)
    }
}

pub struct OperationManifest {
    method: &'static str,
    entries: Vec<OperationManifestEntry>,
}

impl OperationManifest {
    pub const fn new(method: &'static str, entries: Vec<OperationManifestEntry>) -> Self {
        Self { method, entries }
    }

    pub const fn method(&self) -> &'static str {
        self.method
    }

    fn resolve(
        &self,
        capabilities: &CapabilityRegistry,
        arities: &OutArityTable,
        input: &ExpressionDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<(ExpressionDescriptor, &OperationManifestEntry)> {
        self.entries.iter().find_map(|entry| {
            entry
                .resolve(capabilities, arities, input, arguments)
                .map(|output| (output, entry))
        })
    }
}

pub struct OperationManifestEntry {
    input: ExpressionPattern,
    arguments: Vec<ArgumentPattern>,
    output: ExpressionDescriptorTemplate,
    #[cfg(feature = "dynamic")]
    applier: DynApplier,
}

impl OperationManifestEntry {
    pub const fn new(
        input: ExpressionPattern,
        arguments: Vec<ArgumentPattern>,
        output: ExpressionDescriptorTemplate,
        #[cfg(feature = "dynamic")] applier: DynApplier,
    ) -> Self {
        Self {
            input,
            arguments,
            output,
            #[cfg(feature = "dynamic")]
            applier,
        }
    }

    #[must_use]
    pub fn element<S, T>(
        arguments: Vec<ArgumentPattern>,
        emission: EmissionSpec,
        #[cfg(feature = "dynamic")] applier: DynApplier,
    ) -> Self
    where
        S: DescribeShape + ElementShape,
        T: DescribeShape + ElementShape,
    {
        Self::new(
            ExpressionPattern::Lane {
                shape: S::shape_pattern(),
                arity: ArityPattern::Variable(ELEMENT_INPUT_ARITY, Box::new(ArityPattern::Any)),
            },
            arguments,
            ExpressionDescriptorTemplate::Lane {
                shape: T::shape_template(),
                arity: ArityDescriptorTemplate::EmissionOf {
                    input: ELEMENT_INPUT_ARITY,
                    emission,
                },
            },
            #[cfg(feature = "dynamic")]
            applier,
        )
    }

    #[must_use]
    pub fn lane<S, C, T>(
        arguments: Vec<ArgumentPattern>,
        #[cfg(feature = "dynamic")] applier: DynApplier,
    ) -> Self
    where
        S: DescribeShape + ElementShape,
        C: DescribeArity + Arity,
        T: DescribeExpression,
    {
        Self::new(
            <ExpressionHandle<S, C> as DescribeExpression>::expression_pattern(),
            arguments,
            T::expression_template(),
            #[cfg(feature = "dynamic")]
            applier,
        )
    }

    #[must_use]
    pub fn group<M, K, P, T>(
        arguments: Vec<ArgumentPattern>,
        #[cfg(feature = "dynamic")] applier: DynApplier,
    ) -> Self
    where
        M: DescribeIndex + IndexDomain,
        K: DescribeIndex + IndexDomain,
        P: DescribeExpression + Expression,
        T: DescribeExpression,
    {
        Self::new(
            <GroupedExpression<M, K, P> as DescribeExpression>::expression_pattern(),
            arguments,
            T::expression_template(),
            #[cfg(feature = "dynamic")]
            applier,
        )
    }

    fn resolve(
        &self,
        capabilities: &CapabilityRegistry,
        arities: &OutArityTable,
        input: &ExpressionDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<ExpressionDescriptor> {
        if arguments.len() != self.arguments.len() {
            return None;
        }

        let mut bindings = self.input.matches(input, capabilities)?;

        self.arguments
            .iter()
            .zip(arguments)
            .all(|(pattern, argument)| pattern.matches(argument, capabilities, &mut bindings))
            .then(|| self.output.fill(&bindings, capabilities, arities))
    }
}

#[cfg(test)]
mod test {
    use super::OperationRegistry;
    use crate::{
        Mask, Scalar,
        cast::{Bool, Int},
        registry::{
            ArgumentDescriptor, ArityDescriptor, ExpressionDescriptor, IndexDescriptor,
            LaneShapeDescriptor, OrderDescriptor, ValueArgumentDescriptor, ValueDescriptor,
        },
    };
    use graphrecords_core::graphrecord::{AttributeName, NodeIndex, Value};

    fn create_scalar_nodes() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<Scalar>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_mask_nodes() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<Mask>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_attribute_nodes() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<AttributeName>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_mask_values() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Bare {
                value: ValueDescriptor::value::<Mask>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_scalar_value() -> ExpressionDescriptor {
        ExpressionDescriptor::Lane {
            shape: LaneShapeDescriptor::Bare {
                value: ValueDescriptor::value::<Scalar>(),
            },
            arity: ArityDescriptor::Single,
        }
    }

    fn create_grouped_scalar_nodes() -> ExpressionDescriptor {
        ExpressionDescriptor::Group {
            member: IndexDescriptor::domain::<NodeIndex>(),
            key: IndexDescriptor::domain::<Value>(),
            payload: Box::new(create_scalar_nodes()),
        }
    }

    fn create_grouped_mask_nodes() -> ExpressionDescriptor {
        ExpressionDescriptor::Group {
            member: IndexDescriptor::domain::<NodeIndex>(),
            key: IndexDescriptor::domain::<Value>(),
            payload: Box::new(create_mask_nodes()),
        }
    }

    fn create_grouped_scalar_value() -> ExpressionDescriptor {
        ExpressionDescriptor::Group {
            member: IndexDescriptor::domain::<NodeIndex>(),
            key: IndexDescriptor::domain::<Value>(),
            payload: Box::new(create_scalar_value()),
        }
    }

    #[test]
    fn test_resolve() {
        let registry = OperationRegistry::builtins();

        assert_eq!(
            Some(create_scalar_value()),
            registry.resolve("sum", &create_scalar_nodes())
        );
        assert_eq!(
            Some(create_scalar_value()),
            registry.resolve("max", &create_scalar_nodes())
        );
        assert_eq!(
            Some(create_mask_nodes()),
            registry.resolve("is_duplicated", &create_scalar_nodes())
        );
        assert_eq!(
            Some(create_mask_values()),
            registry.resolve("is_duplicated", &create_mask_values())
        );
    }

    #[test]
    fn test_invalid_resolve() {
        let registry = OperationRegistry::builtins();

        assert_eq!(None, registry.resolve("lorem", &create_scalar_nodes()));
        assert_eq!(None, registry.resolve("sum", &create_mask_values()));
        assert_eq!(None, registry.resolve("first", &create_scalar_nodes()));
        assert_eq!(
            None,
            registry.resolve("discard_index", &create_mask_values())
        );
    }

    #[test]
    fn test_resolve_with_arguments() {
        let registry = OperationRegistry::builtins();

        assert_eq!(
            Some(create_attribute_nodes()),
            registry.resolve_with_arguments(
                "cast",
                &create_attribute_nodes(),
                &[ArgumentDescriptor::selector::<Int>()],
            )
        );
        assert_eq!(
            Some(create_grouped_scalar_nodes()),
            registry.resolve_with_arguments(
                "group_by",
                &create_scalar_nodes(),
                &[ArgumentDescriptor::Value(ValueArgumentDescriptor::literal(
                    ValueDescriptor::value::<Scalar>()
                ))],
            )
        );
        assert_eq!(
            Some(create_mask_nodes()),
            registry.resolve_with_arguments(
                "is_in",
                &create_scalar_nodes(),
                &[ArgumentDescriptor::Value(ValueArgumentDescriptor::literal(
                    ValueDescriptor::value::<Scalar>()
                ))],
            )
        );
    }

    #[test]
    fn test_invalid_resolve_with_arguments() {
        let registry = OperationRegistry::builtins();

        assert_eq!(None, registry.resolve("cast", &create_attribute_nodes()));
        assert_eq!(
            None,
            registry.resolve_with_arguments(
                "cast",
                &create_attribute_nodes(),
                &[ArgumentDescriptor::selector::<Bool>()],
            )
        );
        assert_eq!(None, registry.resolve("group_by", &create_scalar_nodes()));
        assert_eq!(None, registry.resolve("is_in", &create_scalar_nodes()));
    }

    #[test]
    fn test_resolve_group() {
        let registry = OperationRegistry::builtins();

        assert_eq!(
            Some(create_grouped_scalar_value()),
            registry.resolve("sum", &create_grouped_scalar_nodes())
        );
        assert_eq!(
            Some(create_grouped_mask_nodes()),
            registry.resolve("is_duplicated", &create_grouped_scalar_nodes())
        );
    }

    #[test]
    fn test_invalid_resolve_group() {
        let registry = OperationRegistry::builtins();

        assert_eq!(None, registry.resolve("sum", &create_grouped_mask_nodes()));
        assert_eq!(
            None,
            registry.resolve("lorem", &create_grouped_scalar_nodes())
        );
    }

    #[test]
    fn test_method_names() {
        let registry = OperationRegistry::builtins();

        let method_names: Vec<_> = registry.method_names().collect();

        assert_eq!(144, method_names.len());
        assert!(method_names.contains(&"sum"));
        assert!(method_names.contains(&"add"));
        assert!(method_names.contains(&"equal_to"));
        assert!(method_names.contains(&"cast"));
        assert!(method_names.contains(&"on_error_raise"));
        assert!(method_names.contains(&"group_by"));
        assert!(method_names.contains(&"index"));
        assert!(method_names.contains(&"is_null"));
        assert!(method_names.contains(&"and"));
        assert!(method_names.contains(&"is_in"));
        assert!(method_names.contains(&"abs"));
        assert!(method_names.contains(&"sort"));
        assert!(method_names.contains(&"uppercase"));
        assert!(method_names.contains(&"attribute"));
        assert!(method_names.contains(&"neighbors"));
        assert!(method_names.contains(&"unique"));
    }
}
