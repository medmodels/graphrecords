mod alias;
pub mod describe;
mod element;
mod group;
mod lane;
pub mod witness;

use super::{
    ArgumentDescriptor, ArgumentPattern, ArityDescriptorTemplate, ArityPattern, CapabilityRegistry,
    EmissionSpec, OperandDescriptor, OperandDescriptorTemplate, OutArityTable, StatePattern,
};
#[cfg(feature = "dynamic")]
use crate::dynamic::DynApplier;
use crate::{
    IndexDomain,
    element::{Arity, ElementShape},
    index::GroupKey,
    operands::{GroupOperand, Operand, OperandHandle},
    operations::operation_manifests,
};
pub(crate) use alias::{
    manifest_entry_alias, manifest_entry_aliases, manifest_entry_argument_pattern,
    manifest_entry_set_argument_pattern, manifest_witness_alias, manifest_witness_argument_alias,
    manifest_witness_set_argument_alias,
};
use describe::{DescribeArity, DescribeIndex, DescribeOperand, DescribeShape};
pub(crate) use element::{
    operation_element_entry, operation_element_method, operation_element_witness,
};
pub(crate) use group::{operation_group_entry, operation_group_witness};
pub(crate) use lane::{operation_lane_entry, operation_lane_witness};
pub use witness::{
    AbsoluteCapability, AddCapability, ArgumentWitness, ArityWitness, BareValueCapability,
    CastBoolCapability, CastDateTimeCapability, CastDurationCapability, CastFloatCapability,
    CastIntCapability, CastStringCapability, CeilCapability, ClipCapability, CubeRootCapability,
    DivideCapability, ElementShapeWitness, EntityAttributesWitness, EntityWitness,
    EnumerableArityWitness, EqualityCapability, EquivalenceCapability, ExponentialCapability,
    FloorCapability, GroupKeyWitness, GroupMemberWitness, GroupingCapability, IndexWitness,
    IndicesInGroupWitness, IntCapability, KindTestCapability, LogarithmCapability,
    MedianCapability, ModeCapability, ModuloCapability, MultiplyCapability, NegateCapability,
    OrderingCapability, PowerCapability, RoundCapability, ScalarCapability,
    ScalarKindTestCapability, SetSourceWitness, SignCapability, SortableCapability,
    SortableIndexWitness, SquareRootCapability, StringCapability, SubtractCapability,
    ValueDomainCapability, ValueDomainOnly, ValueWitness,
};
pub(crate) use witness::{operation_value_capability_marker, operation_value_capability_witness};

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
    pub fn resolve(&self, method: &str, input: &OperandDescriptor) -> Option<OperandDescriptor> {
        self.resolve_with_arguments(method, input, &[])
    }

    #[must_use]
    pub fn resolve_with_arguments(
        &self,
        method: &str,
        input: &OperandDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<OperandDescriptor> {
        self.resolve_entry(method, input, arguments)
            .map(|resolved| resolved.0)
    }

    #[cfg(feature = "dynamic")]
    pub(crate) fn resolve_dispatch(
        &self,
        method: &str,
        input: &OperandDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<(OperandDescriptor, DynApplier)> {
        self.resolve_entry(method, input, arguments)
            .map(|(output, entry)| (output, entry.applier))
    }

    fn resolve_entry(
        &self,
        method: &str,
        input: &OperandDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<(OperandDescriptor, &OperationManifestEntry)> {
        if let Some(resolved) = self.resolve_here(method, input, arguments) {
            return Some(resolved);
        }

        let OperandDescriptor::Group {
            member,
            key,
            payload,
        } = input
        else {
            return None;
        };
        let (payload, entry) = self.resolve_entry(method, payload, arguments)?;
        let output = OperandDescriptor::Group {
            member: member.clone(),
            key: key.clone(),
            payload: Box::new(payload),
        };

        Some((output, entry))
    }

    fn resolve_here(
        &self,
        method: &str,
        input: &OperandDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<(OperandDescriptor, &OperationManifestEntry)> {
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
        input: &OperandDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<(OperandDescriptor, &OperationManifestEntry)> {
        self.entries.iter().find_map(|entry| {
            entry
                .resolve(capabilities, arities, input, arguments)
                .map(|output| (output, entry))
        })
    }
}

pub struct OperationManifestEntry {
    input: StatePattern,
    arguments: Vec<ArgumentPattern>,
    output: OperandDescriptorTemplate,
    #[cfg(feature = "dynamic")]
    applier: DynApplier,
}

impl OperationManifestEntry {
    pub const fn new(
        input: StatePattern,
        arguments: Vec<ArgumentPattern>,
        output: OperandDescriptorTemplate,
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
            StatePattern::Lane {
                shape: S::shape_pattern(),
                arity: ArityPattern::Variable(ELEMENT_INPUT_ARITY, Box::new(ArityPattern::Any)),
            },
            arguments,
            OperandDescriptorTemplate::Lane {
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
        T: DescribeOperand,
    {
        Self::new(
            <OperandHandle<S, C> as DescribeOperand>::state_pattern(),
            arguments,
            T::operand_template(),
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
        K: DescribeIndex + GroupKey,
        P: DescribeOperand + Operand,
        T: DescribeOperand,
    {
        Self::new(
            <GroupOperand<M, K, P> as DescribeOperand>::state_pattern(),
            arguments,
            T::operand_template(),
            #[cfg(feature = "dynamic")]
            applier,
        )
    }

    fn resolve(
        &self,
        capabilities: &CapabilityRegistry,
        arities: &OutArityTable,
        input: &OperandDescriptor,
        arguments: &[ArgumentDescriptor],
    ) -> Option<OperandDescriptor> {
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

macro_rules! operation_manifest_name {
    ($registry_name:literal $method:ident) => {
        $registry_name
    };
    ($method:ident) => {
        stringify!($method)
    };
}

macro_rules! operation_policy_method {
    (
        OnError,
        $method:ident,
        policy[$policy:path $(= $($constructor:tt)+)?],
        $receiver:ty
    ) => {
        const fn verify_method<O: OnError>()
        where
            $policy: $crate::operations::ErrorPolicy<O>,
        {
            let _ = O::$method::<$policy>;
        }

        verify_method::<$receiver>();
    };
    (
        OnBucketError,
        $method:ident,
        policy[$policy:path $(= $($constructor:tt)+)?],
        $receiver:ty
    ) => {
        const fn verify_method<O: OnBucketError>()
        where
            $policy: $crate::operations::BucketErrorPolicy<O>,
        {
            let _ = O::$method::<$policy>;
        }

        verify_method::<$receiver>();
    };
    (
        OnKeyError,
        $method:ident,
        policy[$policy:path $(= $($constructor:tt)+)?],
        $receiver:ty
    ) => {
        const fn verify_method<O: OnKeyError>()
        where
            $policy: $crate::operations::KeyErrorPolicy<O>,
        {
            let _ = O::$method::<$policy>;
        }

        verify_method::<$receiver>();
    };
    (
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        policy[],
        $receiver:ty
    ) => {
        const fn verify_method<O: $trait $(<$($trait_argument),+>)?>() {
            let _ = O::$method;
        }

        verify_method::<$receiver>();
    };
}

macro_rules! operation_manifest {
    (
        $operation:ty $(as $registry_name:literal)? {
            method: $trait:ident $(<$($trait_argument:ty),+ $(,)?>)? :: $method:ident;
            $(policy: $policy:path $(= $owner:ident $access:tt $function:ident($argument:ident))?;)?
            scope: $scope:ident;

            kernel $first_kernel:tt
            $(kernel $additional_kernel:tt)*
        }
    ) => {
        $crate::registry::operation_manifest!(
            @scope $scope,
            $operation,
            trait[$trait $(<$($trait_argument),+>)?],
            $method,
            name[$($registry_name)? $method],
            policy[$($policy $(= $owner $access $function($argument))?)?],
            $first_kernel
            $($additional_kernel)*
        );
    };
    (@scope element, $($manifest:tt)*) => {
        $crate::registry::operation_manifest!(
            @entries operation_element_witness, operation_element_entry, $($manifest)*
        );
    };
    (@scope lane, $($manifest:tt)*) => {
        $crate::registry::operation_manifest!(
            @entries operation_lane_witness, operation_lane_entry, $($manifest)*
        );
    };
    (@scope group, $($manifest:tt)*) => {
        $crate::registry::operation_manifest!(
            @entries operation_group_witness, operation_group_entry, $($manifest)*
        );
    };
    (
        @entries $witness:ident, $entry:ident,
        $operation:ty,
        trait[$($trait:tt)+],
        $method:ident,
        name[$($name:tt)+],
        policy $policy:tt,
        $first_kernel:tt
        $($additional_kernel:tt)*
    ) => {
        const _: () = {
            $crate::registry::$witness!(
                $operation,
                $($trait)+,
                $method,
                policy $policy,
                $first_kernel
                $($additional_kernel)*
            );
        };

        pub fn operation_manifest() -> $crate::registry::OperationManifest {
            $crate::registry::OperationManifest::new(
                $crate::registry::operation_manifest_name!($($name)+),
                vec![
                    $crate::registry::$entry!(
                        $operation,
                        $method,
                        policy $policy,
                        $first_kernel,
                        $first_kernel
                    )
                    $(,
                        $crate::registry::$entry!(
                            $operation,
                            $method,
                            policy $policy,
                            $additional_kernel,
                            $additional_kernel
                        )
                    )*
                ],
            )
        }
    };
}

pub(crate) use operation_manifest;
pub(crate) use operation_manifest_name;
pub(crate) use operation_policy_method;

#[cfg(test)]
mod test {
    use super::OperationRegistry;
    use crate::{
        Mask, Scalar,
        cast::{Bool, Int},
        registry::{
            ArgumentDescriptor, ArityDescriptor, IndexDescriptor, LaneShapeDescriptor,
            OperandDescriptor, OrderDescriptor, ValueArgumentDescriptor, ValueDescriptor,
        },
    };
    use graphrecords_core::graphrecord::{AttributeName, NodeIndex, Value};

    fn create_scalar_nodes() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<Scalar>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_mask_nodes() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<Mask>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_attribute_nodes() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Indexed {
                index: IndexDescriptor::domain::<NodeIndex>(),
                value: ValueDescriptor::value::<AttributeName>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_mask_values() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Bare {
                value: ValueDescriptor::value::<Mask>(),
            },
            arity: ArityDescriptor::Multiple {
                order: OrderDescriptor::Unordered,
            },
        }
    }

    fn create_scalar_value() -> OperandDescriptor {
        OperandDescriptor::Lane {
            shape: LaneShapeDescriptor::Bare {
                value: ValueDescriptor::value::<Scalar>(),
            },
            arity: ArityDescriptor::Single,
        }
    }

    fn create_grouped_scalar_nodes() -> OperandDescriptor {
        OperandDescriptor::Group {
            member: IndexDescriptor::domain::<NodeIndex>(),
            key: IndexDescriptor::domain::<Value>(),
            payload: Box::new(create_scalar_nodes()),
        }
    }

    fn create_grouped_mask_nodes() -> OperandDescriptor {
        OperandDescriptor::Group {
            member: IndexDescriptor::domain::<NodeIndex>(),
            key: IndexDescriptor::domain::<Value>(),
            payload: Box::new(create_mask_nodes()),
        }
    }

    fn create_grouped_scalar_value() -> OperandDescriptor {
        OperandDescriptor::Group {
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

        // Resolving a method that is not registered should fail
        assert_eq!(None, registry.resolve("lorem", &create_scalar_nodes()));

        // Summing masks should fail
        assert_eq!(None, registry.resolve("sum", &create_mask_values()));

        // Taking the first element of an unordered lane should fail
        assert_eq!(None, registry.resolve("first", &create_scalar_nodes()));

        // Discarding the index of a bare lane should fail
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

        // Casting without a cast target should fail
        assert_eq!(None, registry.resolve("cast", &create_attribute_nodes()));

        // Casting attribute names to booleans should fail
        assert_eq!(
            None,
            registry.resolve_with_arguments(
                "cast",
                &create_attribute_nodes(),
                &[ArgumentDescriptor::selector::<Bool>()],
            )
        );

        // Grouping without a key should fail
        assert_eq!(None, registry.resolve("group_by", &create_scalar_nodes()));

        // Testing membership without a set should fail
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

        // Summing a group of masks should fail
        assert_eq!(None, registry.resolve("sum", &create_grouped_mask_nodes()));

        // Resolving a method that is not registered should fail on a group too
        assert_eq!(
            None,
            registry.resolve("lorem", &create_grouped_scalar_nodes())
        );
    }

    #[test]
    fn test_method_names() {
        let registry = OperationRegistry::builtins();

        let method_names: Vec<_> = registry.method_names().collect();

        assert_eq!(127, method_names.len());
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
