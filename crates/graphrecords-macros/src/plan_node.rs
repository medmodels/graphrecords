use crate::crate_path;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Fields, Index, Result, Type};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;

    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();

    let mut operand: Option<Type> = None;

    let crate_path = crate_path(&input.attrs, "plan_node", |meta| {
        if meta.path.is_ident("operand") {
            operand = Some(meta.value()?.parse::<Type>()?);
            Ok(true)
        } else {
            Ok(false)
        }
    })?;

    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            input,
            "PlanNode can only be derived for structs",
        ));
    };

    let mut inputs = Vec::new();
    let mut input_types = Vec::new();
    let mut payload = Vec::new();

    for (index, field) in data.fields.iter().enumerate() {
        let accessor = if let Some(identifier) = &field.ident {
            quote!(#identifier)
        } else {
            let index = Index::from(index);
            quote!(#index)
        };

        let mut is_input = false;

        for attribute in &field.attrs {
            if !attribute.path().is_ident("plan_node") {
                continue;
            }

            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("input") {
                    is_input = true;
                    Ok(())
                } else {
                    Err(meta.error("unknown plan_node field attribute"))
                }
            })?;
        }

        if is_input {
            inputs.push(accessor);
            input_types.push(field.ty.clone());

            continue;
        }

        payload.push(accessor);
    }

    let payload_hash = payload.clone();
    let payload_eq = payload.clone();
    let rebuild_payload = payload;
    let has_inputs_accessors = inputs.clone();

    let has_inputs_body = if has_inputs_accessors.is_empty() {
        quote!()
    } else {
        quote!( ( #( &self.#has_inputs_accessors, )* ) )
    };

    let optimized_locals: Vec<_> = (0..inputs.len())
        .map(|index| format_ident!("optimized_input_{index}"))
        .collect();

    let optimized_locals_changed = optimized_locals.clone();
    let optimized_locals_value = optimized_locals.clone();
    let inputs_to_optimize = inputs.clone();
    let inputs_to_rebuild = inputs.clone();

    let optimize_inputs_impl = match &operand {
        Some(operand) => {
            let body = if matches!(data.fields, Fields::Unit) {
                quote! {
                    #crate_path::optimizer::Transformed::unchanged(::core::clone::Clone::clone(original))
                }
            } else {
                quote! {
                    #( let #optimized_locals = session.optimize(&self.#inputs_to_optimize); )*

                    let changed = false #( || #optimized_locals_changed.changed )*;

                    if !changed {
                        return #crate_path::optimizer::Transformed::unchanged(
                            ::core::clone::Clone::clone(original),
                        );
                    }

                    #crate_path::optimizer::Transformed {
                        value: <#operand as #crate_path::Operand>::from_context(::std::sync::Arc::new(Self {
                            #( #inputs_to_rebuild: #optimized_locals_value.value, )*
                            #( #rebuild_payload: self.#rebuild_payload.clone(), )*
                        })),
                        changed: true,
                    }
                }
            };

            quote! {
                impl #impl_generics #crate_path::optimizer::OptimizeInputs for #name #type_generics #where_clause {
                    type Output = #operand;

                    fn optimize_inputs(
                        &self,
                        original: &Self::Output,
                        session: &#crate_path::optimizer::Session,
                    ) -> #crate_path::optimizer::Transformed<Self::Output> {
                        #body
                    }
                }
            }
        }
        None => quote!(),
    };

    Ok(quote! {

        #optimize_inputs_impl

        impl #impl_generics #crate_path::optimizer::HasInputs for #name #type_generics #where_clause {
            type Inputs<'inputs> = ( #( &'inputs #input_types, )* );

            fn inputs(&self) -> Self::Inputs<'_> {
                #has_inputs_body
            }
        }

        const _: () = {
            use #crate_path::Operand as _Operand;
            use #crate_path::optimizer::PlanNode as _PlanNode;

            impl #impl_generics _PlanNode for #name #type_generics #where_clause {
                fn inputs(&self) -> ::std::vec::Vec<&dyn _PlanNode> {
                    let mut inputs: ::std::vec::Vec<&dyn _PlanNode> = ::std::vec::Vec::new();
                    #( inputs.push(_Operand::context(&self.#inputs)); )*
                    inputs
                }

                fn dyn_eq(&self, other: &dyn _PlanNode) -> bool {
                    let ::std::option::Option::Some(other) = other.downcast::<Self>() else {
                        return false;
                    };

                    #(
                        if self.#payload_eq != other.#payload_eq {
                            return false;
                        }
                    )*

                    _PlanNode::inputs(self)
                        .into_iter()
                        .zip(_PlanNode::inputs(other))
                        .all(|(self_input, other_input)| self_input.dyn_eq(other_input))
                }

                fn dyn_hash(&self, mut state: &mut dyn ::std::hash::Hasher) {
                    ::std::hash::Hash::hash(&::std::any::Any::type_id(self), &mut state);
                    #( ::std::hash::Hash::hash(&self.#payload_hash, &mut state); )*

                    for input in _PlanNode::inputs(self) {
                        input.dyn_hash(state);
                    }
                }
            }
        };
    })
}
