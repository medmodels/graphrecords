macro_rules! operation_lane_witness {
    (
        $operation:ty,
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        policy[$($policy:tt)*],
    ) => {};
    (
        $operation:ty,
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        policy[$($policy:tt)*],
        $kernel:tt
        $($remaining:tt)*
    ) => {
        $crate::registry::operation_lane_witness!(
            @kernel
            $operation,
            $trait $(<$($trait_argument),+>)?,
            $method,
            policy[$($policy)*],
            $kernel
        );
        $crate::registry::operation_lane_witness!(
            $operation,
            $trait $(<$($trait_argument),+>)?,
            $method,
            policy[$($policy)*],
            $($remaining)*
        );
    };
    (
        @kernel
        $operation:ty,
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        policy[$($policy:tt)*], {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ty;)*
            argument: $argument:ident :
                ArgumentSource<$alignment:ty, $value:ty, Retention = $retention:ty>;
            input: ($shape:ty, $arity:ty);
            output: $output:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        {
            $($crate::registry::manifest_witness_alias!($crate::Ordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $crate::registry::manifest_witness_argument_alias!(
                $argument, $alignment, $value; retention $retention
            );

            const fn verify_scope()
            where
                $operation: $crate::operations::Operation<Scope = $crate::operations::Lane>,
            {
            }

            const fn verify_kernel()
            where
                $operation: $crate::operations::LaneKernel<$shape, $arity, Output = $output>,
                $(<$where_owner as $crate::ValueDomain>::Owned: $where_first $(+ $where_more)*,)?
            {
            }

            verify_scope();
            verify_kernel();
            $crate::registry::operation_policy_method!(
                $trait $(<$($trait_argument),+>)?,
                $method,
                policy[$($policy)*],
                $crate::operands::OperandHandle<$shape, $arity>
            );
        }
        {
            $($crate::registry::manifest_witness_alias!($crate::Unordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $crate::registry::manifest_witness_argument_alias!(
                $argument, $alignment, $value; retention $retention
            );

            const fn verify_kernel()
            where
                $operation: $crate::operations::LaneKernel<$shape, $arity, Output = $output>,
                $(<$where_owner as $crate::ValueDomain>::Owned: $where_first $(+ $where_more)*,)?
            {
            }

            verify_kernel();
        }
    }};
    (
        @kernel
        $operation:ty,
        $trait:ident $(<$($trait_argument:ty),+ $(,)?>)?,
        $method:ident,
        policy[$($policy:tt)*], {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ty;)*
            $(
                argument: $argument:ident : ArgumentSource<$alignment:ty $(, $value:ty)?>
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: ($shape:ty, $arity:ty);
            output: $output:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        {
            $($crate::registry::manifest_witness_alias!($crate::Ordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $(
                $crate::registry::manifest_witness_argument_alias!(
                    $argument, $alignment $(, $value)? $(; $capability)?
                );
            )*

            const fn verify_scope()
            where
                $operation: $crate::operations::Operation<Scope = $crate::operations::Lane>,
            {
            }

            const fn verify_kernel()
            where
                $operation: $crate::operations::LaneKernel<$shape, $arity, Output = $output>,
                $(<$where_owner as $crate::ValueDomain>::Owned: $where_first $(+ $where_more)*,)?
            {
            }

            verify_scope();
            verify_kernel();
            $crate::registry::operation_policy_method!(
                $trait $(<$($trait_argument),+>)?,
                $method,
                policy[$($policy)*],
                $crate::operands::OperandHandle<$shape, $arity>
            );
        }
        {
            $($crate::registry::manifest_witness_alias!($crate::Unordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $(
                $crate::registry::manifest_witness_argument_alias!(
                    $argument, $alignment $(, $value)? $(; $capability)?
                );
            )*

            const fn verify_kernel()
            where
                $operation: $crate::operations::LaneKernel<$shape, $arity, Output = $output>,
                $(<$where_owner as $crate::ValueDomain>::Owned: $where_first $(+ $where_more)*,)?
            {
            }

            verify_kernel();
        }
    }};
}

macro_rules! operation_lane_entry {
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        $dynamic_kernel:tt,
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ident;)*
            argument: $argument:ident :
                ArgumentSource<$alignment:ty, $value:ty, Retention = $retention:ty>;
            input: ($shape:ty, $arity:ty);
            output: $output:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        $crate::registry::manifest_entry_aliases!(
            (0)
            $($name : $bound $(<$target>)? $(+ $additional)*,)*
        );

        $crate::registry::OperationManifestEntry::lane::<$shape, $arity, $output>(
            vec![
                $($crate::registry::ArgumentPattern::field::<$field_type>(),)*
                $crate::registry::manifest_entry_argument_pattern!($alignment, $value),
            ],
            #[cfg(feature = "dynamic")]
            $crate::dynamic::operation_lane_applier!(
                $operation,
                $method,
                policy[$($policy)*],
                $dynamic_kernel
            ),
        )
    }};
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        $dynamic_kernel:tt,
        {
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(field: $field:ident : $field_type:ident;)*
            $(
                argument: $argument:ident : ArgumentSource<$alignment:ty $(, $value:ty)?>
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: ($shape:ty, $arity:ty);
            output: $output:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        $crate::registry::manifest_entry_aliases!(
            (0)
            $($name : $bound $(<$target>)? $(+ $additional)*,)*
        );

        $crate::registry::OperationManifestEntry::lane::<$shape, $arity, $output>(
            vec![
                $($crate::registry::ArgumentPattern::field::<$field_type>(),)*
                $(
                    $crate::registry::manifest_entry_argument_pattern!(
                        $alignment $(, $value)? $(; $capability)?
                    ),
                )*
            ],
            #[cfg(feature = "dynamic")]
            $crate::dynamic::operation_lane_applier!(
                $operation,
                $method,
                policy[$($policy)*],
                $dynamic_kernel
            ),
        )
    }};
}

pub(crate) use operation_lane_entry;
pub(crate) use operation_lane_witness;
