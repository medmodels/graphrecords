macro_rules! operation_group_witness {
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
        $crate::registry::operation_group_witness!(
            @kernel
            $operation,
            $trait $(<$($trait_argument),+>)?,
            $method,
            policy[$($policy)*],
            $kernel
        );
        $crate::registry::operation_group_witness!(
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
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            argument: $argument:ident :
                IndexedElementSource<Indexed<$via_index:ty, $via_value:ty>, $via_arity:ty>;
            input: $payload:ty;
            output: $output:ty;
        }
    ) => {{
        {
            $crate::registry::manifest_witness_alias!($crate::Ordered, $member, GroupMember);
            $crate::registry::manifest_witness_alias!($crate::Ordered, $key, GroupKey);
            $($crate::registry::manifest_witness_alias!($crate::Ordered, $name, $bound $(<$target>)? $(+ $additional)*);)*

            type $argument = $crate::operands::OperandHandle<
                $crate::Indexed<$via_index, $via_value>,
                $via_arity,
            >;

            const fn verify_scope()
            where
                $operation: $crate::operations::Operation<Scope = $crate::operations::Group>,
            {
            }

            const fn verify_kernel()
            where
                $operation: $crate::operations::GroupKernel<
                    $member,
                    $key,
                    $payload,
                    Output = $output,
                >,
            {
            }

            verify_scope();
            verify_kernel();
            $crate::registry::operation_policy_method!(
                $trait $(<$($trait_argument),+>)?,
                $method,
                policy[$($policy)*],
                $crate::operands::GroupOperand<$member, $key, $payload>
            );
        }
        {
            $crate::registry::manifest_witness_alias!($crate::Unordered, $member, GroupMember);
            $crate::registry::manifest_witness_alias!($crate::Unordered, $key, GroupKey);
            $($crate::registry::manifest_witness_alias!($crate::Unordered, $name, $bound $(<$target>)? $(+ $additional)*);)*

            type $argument = $crate::operands::OperandHandle<
                $crate::Indexed<$via_index, $via_value>,
                $via_arity,
            >;

            const fn verify_kernel()
            where
                $operation: $crate::operations::GroupKernel<
                    $member,
                    $key,
                    $payload,
                    Output = $output,
                >,
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
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(
                argument: $argument:ident : ArgumentSource<$alignment:ty $(, $value:ty)?>
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: $payload:ty;
            output: $output:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        {
            $crate::registry::manifest_witness_alias!($crate::Ordered, $member, GroupMember);
            $crate::registry::manifest_witness_alias!($crate::Ordered, $key, GroupKey);
            $($crate::registry::manifest_witness_alias!($crate::Ordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $(
                $crate::registry::manifest_witness_argument_alias!(
                    $argument, $alignment $(, $value)? $(; $capability)?
                );
            )*

            const fn verify_scope()
            where
                $operation: $crate::operations::Operation<Scope = $crate::operations::Group>,
            {
            }

            const fn verify_kernel()
            where
                $operation: $crate::operations::GroupKernel<
                    $member,
                    $key,
                    $payload,
                    Output = $output,
                >,
                $(<$where_owner as $crate::ValueDomain>::Owned: $where_first $(+ $where_more)*,)?
            {
            }

            verify_scope();
            verify_kernel();
            $crate::registry::operation_policy_method!(
                $trait $(<$($trait_argument),+>)?,
                $method,
                policy[$($policy)*],
                $crate::operands::GroupOperand<$member, $key, $payload>
            );
        }
        {
            $crate::registry::manifest_witness_alias!($crate::Unordered, $member, GroupMember);
            $crate::registry::manifest_witness_alias!($crate::Unordered, $key, GroupKey);
            $($crate::registry::manifest_witness_alias!($crate::Unordered, $name, $bound $(<$target>)? $(+ $additional)*);)*
            $(
                $crate::registry::manifest_witness_argument_alias!(
                    $argument, $alignment $(, $value)? $(; $capability)?
                );
            )*

            const fn verify_kernel()
            where
                $operation: $crate::operations::GroupKernel<
                    $member,
                    $key,
                    $payload,
                    Output = $output,
                >,
                $(<$where_owner as $crate::ValueDomain>::Owned: $where_first $(+ $where_more)*,)?
            {
            }

            verify_kernel();
        }
    }};
}

macro_rules! operation_group_entry {
    (
        $operation:ty,
        $method:ident,
        policy[$($policy:tt)*],
        $dynamic_kernel:tt,
        {
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            argument: $argument:ident :
                IndexedElementSource<Indexed<$via_index:ty, $via_value:ty>, $via_arity:ty>;
            input: $payload:ty;
            output: $output:ty;
        }
    ) => {{
        $crate::registry::manifest_entry_aliases!(
            (0)
            $member : GroupMember,
            $key : GroupKey,
            $($name : $bound $(<$target>)? $(+ $additional)*,)*
        );

        $crate::registry::OperationManifestEntry::group::<$member, $key, $payload, $output>(
            vec![
                $crate::registry::ArgumentPattern::Operand(
                    <$crate::operands::OperandHandle<
                        $crate::Indexed<$via_index, $via_value>,
                        $via_arity,
                    > as $crate::registry::describe::DescribeOperand>::state_pattern(),
                ),
            ],
            #[cfg(feature = "dynamic")]
            $crate::dynamic::operation_group_applier!(
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
            group: < $member:ident : IndexDomain, $key:ident : GroupKey $(,)? >;
            parameters: < $($name:ident : $bound:ident $(<$target:ident>)? $(+ $additional:ident)*),* $(,)? >;
            $(
                argument: $argument:ident : ArgumentSource<$alignment:ty $(, $value:ty)?>
                $(where $argument_owner:ident::ValueDomain : $capability:ident)?;
            )*
            input: $payload:ty;
            output: $output:ty;
            $(where $where_owner:ident::Owned : $where_first:ident $(+ $where_more:ident)*;)?
        }
    ) => {{
        $crate::registry::manifest_entry_aliases!(
            (0)
            $member : GroupMember,
            $key : GroupKey,
            $($name : $bound $(<$target>)? $(+ $additional)*,)*
        );

        $crate::registry::OperationManifestEntry::group::<$member, $key, $payload, $output>(
            vec![
                $(
                    $crate::registry::manifest_entry_argument_pattern!(
                        $alignment $(, $value)? $(; $capability)?
                    ),
                )*
            ],
            #[cfg(feature = "dynamic")]
            $crate::dynamic::operation_group_applier!(
                $operation,
                $method,
                policy[$($policy)*],
                $dynamic_kernel
            ),
        )
    }};
}

pub(crate) use operation_group_entry;
pub(crate) use operation_group_witness;
