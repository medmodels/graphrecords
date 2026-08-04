macro_rules! manifest_witness_alias {
    ($order:ty, $name:ident,IndexDomain) => {
        type $name = $crate::registry::IndexWitness;
    };
    ($order:ty, $name:ident,EntityDomain) => {
        type $name = $crate::registry::EntityWitness;
    };
    ($order:ty, $name:ident,EntityAttributes) => {
        type $name = $crate::registry::EntityAttributesWitness;
    };
    ($order:ty, $name:ident,IndicesInGroup) => {
        type $name = $crate::registry::IndicesInGroupWitness;
    };
    ($order:ty, $name:ident,EnsureSortable) => {
        type $name = $crate::registry::SortableIndexWitness;
    };
    ($order:ty, $name:ident,GroupMember) => {
        type $name = $crate::registry::GroupMemberWitness;
    };
    ($order:ty, $name:ident,GroupKey) => {
        type $name = $crate::registry::GroupKeyWitness;
    };
    ($order:ty, $name:ident,ElementShape) => {
        type $name = $crate::registry::ElementShapeWitness;
    };
    ($order:ty, $name:ident,OrderState) => {
        type $name = $order;
    };
    ($order:ty, $name:ident,Arity) => {
        type $name = $crate::registry::ArityWitness;
    };
    ($order:ty, $name:ident,EnumerableArity) => {
        type $name = $crate::registry::EnumerableArityWitness;
    };
    ($order:ty, $name:ident,Lane) => {
        type $name = $crate::operands::OperandHandle<
            $crate::Bare<
                $crate::registry::ValueWitness<
                    $crate::registry::ValueDomainCapability,
                    $crate::registry::BareValueCapability,
                >,
            >,
            $crate::Multiple<$order>,
        >;
    };
    ($order:ty, $name:ident,ValueDomain) => {
        type $name = $crate::registry::ValueWitness<
            $crate::registry::ValueDomainCapability,
            $crate::registry::ValueDomainOnly,
        >;
    };
    ($order:ty, $name:ident,BareValueDomain) => {
        type $name = $crate::registry::ValueWitness<
            $crate::registry::ValueDomainCapability,
            $crate::registry::BareValueCapability,
        >;
    };
    ($order:ty, $name:ident,GroupingValue < $target:ident >) => {
        type $name = $crate::registry::ValueWitness<
            $crate::registry::GroupingCapability,
            $crate::registry::ValueDomainOnly,
        >;
    };
    ($order:ty, $name:ident, $capability:ident < $target:ident > + BareValueDomain) => {
        type $name = $crate::registry::operation_value_capability_witness!(
            $capability<$target>,
            $crate::registry::BareValueCapability
        );
    };
    ($order:ty, $name:ident, $capability:ident < $target:ident >) => {
        type $name = $crate::registry::operation_value_capability_witness!(
            $capability<$target>,
            $crate::registry::ValueDomainOnly
        );
    };
    ($order:ty, $name:ident, $capability:ident + $additional:ident + BareValueDomain) => {
        type $name = $crate::registry::ValueWitness<
            (
                $crate::registry::operation_value_capability_marker!($capability),
                $crate::registry::operation_value_capability_marker!($additional),
            ),
            $crate::registry::BareValueCapability,
        >;
    };
    ($order:ty, $name:ident, $capability:ident + BareValueDomain) => {
        type $name = $crate::registry::operation_value_capability_witness!(
            $capability,
            $crate::registry::BareValueCapability
        );
    };
    ($order:ty, $name:ident, $capability:ident + $additional:ident) => {
        type $name = $crate::registry::ValueWitness<
            (
                $crate::registry::operation_value_capability_marker!($capability),
                $crate::registry::operation_value_capability_marker!($additional),
            ),
            $crate::registry::ValueDomainOnly,
        >;
    };
    ($order:ty, $name:ident, $capability:ident) => {
        type $name = $crate::registry::operation_value_capability_witness!(
            $capability,
            $crate::registry::ValueDomainOnly
        );
    };
}

macro_rules! manifest_entry_argument_pattern {
    ($alignment:ty, $value:ty) => {
        $crate::registry::ArgumentPattern::Value {
            value: <$value as $crate::registry::describe::DescribeValue>::value_pattern(),
            alignment: <$alignment as
                            $crate::registry::describe::DescribeAlignment>::alignment_descriptor(),
        }
    };
    ($alignment:ty; $capability:ident) => {
        $crate::registry::ArgumentPattern::Value {
            value: <$crate::registry::operation_value_capability_marker!($capability) as
                            $crate::registry::describe::CapabilityMarkers>::argument_value_pattern(),
            alignment: <$alignment as
                            $crate::registry::describe::DescribeAlignment>::alignment_descriptor(),
        }
    };
    ($alignment:ty) => {
        $crate::registry::ArgumentPattern::Value {
            value: <$crate::registry::describe::RegisteredOnly as
                            $crate::registry::describe::CapabilityMarkers>::argument_value_pattern(),
            alignment: <$alignment as
                            $crate::registry::describe::DescribeAlignment>::alignment_descriptor(),
        }
    };
}

macro_rules! manifest_entry_set_argument_pattern {
    ($value:ty) => {
        $crate::registry::ArgumentPattern::Set(
            <$value as $crate::registry::describe::DescribeValue>::value_pattern(),
        )
    };
}

macro_rules! manifest_entry_aliases {
    (($($position:tt)*)) => {};
    (
        ($($position:tt)*)
        $name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*,
        $($remaining:tt)*
    ) => {
        $crate::registry::manifest_entry_alias!(
            ($($position)*), $name, $bound $(<$target>)? $(+ $additional)*
        );
        $crate::registry::manifest_entry_aliases!(($($position)* + 1) $($remaining)*);
    };
}

macro_rules! manifest_entry_alias {
    (($($position:tt)*), $name:ident,IndexDomain) => {
        type $name = $crate::registry::describe::IndexPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,EntityDomain) => {
        type $name = $crate::registry::describe::EntityPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,EntityAttributes) => {
        type $name =
            $crate::registry::describe::EntityAttributesPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,IndicesInGroup) => {
        type $name = $crate::registry::describe::IndicesInGroupPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,EnsureSortable) => {
        type $name = $crate::registry::describe::SortableIndexPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,GroupMember) => {
        type $name = $crate::registry::describe::IndexPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,GroupKey) => {
        type $name = $crate::registry::describe::GroupKeyPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,ElementShape) => {
        type $name = $crate::registry::describe::ShapePatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,OrderState) => {
        type $name = $crate::registry::describe::OrderPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,Arity) => {
        type $name = $crate::registry::describe::ArityPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,EnumerableArity) => {
        type $name = $crate::registry::describe::ArityPatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,Lane) => {
        type $name = $crate::registry::describe::LanePatternVariable<{ $($position)* }>;
    };
    (($($position:tt)*), $name:ident,ValueDomain) => {
        type $name = $crate::registry::describe::ValuePatternVariable<
            { $($position)* },
            $crate::registry::describe::RegisteredOnly,
        >;
    };
    (($($position:tt)*), $name:ident,BareValueDomain) => {
        type $name = $crate::registry::describe::ValuePatternVariable<
            { $($position)* },
            $crate::registry::BareValueCapability,
        >;
    };
    (($($position:tt)*), $name:ident,GroupingValue<$target:ident>) => {
        type $name =
            $crate::registry::describe::GroupingValuePatternVariable<{ $($position)* }, $target>;
    };
    (($($position:tt)*), $name:ident, $capability:ident<$target:ident> + BareValueDomain) => {
        type $name = $crate::registry::describe::ValuePatternVariable<
            { $($position)* },
            (
                $crate::registry::operation_value_capability_marker!($capability<$target>),
                $crate::registry::BareValueCapability,
            ),
        >;
    };
    (($($position:tt)*), $name:ident, $capability:ident<$target:ident>) => {
        type $name = $crate::registry::describe::ValuePatternVariable<
            { $($position)* },
            $crate::registry::operation_value_capability_marker!($capability<$target>),
        >;
    };
    (($($position:tt)*), $name:ident, $capability:ident + $additional:ident + BareValueDomain) => {
        type $name = $crate::registry::describe::ValuePatternVariable<
            { $($position)* },
            (
                (
                    $crate::registry::operation_value_capability_marker!($capability),
                    $crate::registry::operation_value_capability_marker!($additional),
                ),
                $crate::registry::BareValueCapability,
            ),
        >;
    };
    (($($position:tt)*), $name:ident, $capability:ident + BareValueDomain) => {
        type $name = $crate::registry::describe::ValuePatternVariable<
            { $($position)* },
            (
                $crate::registry::operation_value_capability_marker!($capability),
                $crate::registry::BareValueCapability,
            ),
        >;
    };
    (($($position:tt)*), $name:ident, $capability:ident + $additional:ident) => {
        type $name = $crate::registry::describe::ValuePatternVariable<
            { $($position)* },
            (
                $crate::registry::operation_value_capability_marker!($capability),
                $crate::registry::operation_value_capability_marker!($additional),
            ),
        >;
    };
    (($($position:tt)*), $name:ident, $capability:ident) => {
        type $name = $crate::registry::describe::ValuePatternVariable<
            { $($position)* },
            $crate::registry::operation_value_capability_marker!($capability),
        >;
    };
}

macro_rules! manifest_witness_argument_alias {
    ($name:ident, $alignment:ty, $value:ty; retention $retention:ty) => {
        type $name = $crate::registry::ArgumentWitness<$alignment, $value, $retention>;
    };
    ($name:ident, $alignment:ty, $value:ty) => {
        type $name = $crate::registry::ArgumentWitness<$alignment, $value>;
    };
    ($name:ident, $alignment:ty; $capability:ident) => {
        type $name = $crate::registry::ArgumentWitness<
            $alignment,
            $crate::registry::operation_value_capability_witness!(
                $capability,
                $crate::registry::ValueDomainOnly
            ),
        >;
    };
    ($name:ident, $alignment:ty) => {
        type $name = $crate::registry::ArgumentWitness<
            $alignment,
            $crate::registry::ValueWitness<
                $crate::registry::ValueDomainCapability,
                $crate::registry::ValueDomainOnly,
            >,
        >;
    };
}

macro_rules! manifest_witness_set_argument_alias {
    ($name:ident, $value:ty) => {
        type $name = $crate::registry::SetSourceWitness<$value>;
    };
}

pub(crate) use manifest_entry_alias;
pub(crate) use manifest_entry_aliases;
pub(crate) use manifest_entry_argument_pattern;
pub(crate) use manifest_entry_set_argument_pattern;
pub(crate) use manifest_witness_alias;
pub(crate) use manifest_witness_argument_alias;
pub(crate) use manifest_witness_set_argument_alias;
