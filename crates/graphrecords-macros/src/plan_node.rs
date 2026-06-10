use crate::plan::PlanModel;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        generics,
        crate_path,
        inputs,
        payload,
        ..
    } = PlanModel::parse(input)?;

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let payload_eq = payload.clone();
    let payload_hash = payload;

    Ok(quote! {
        const _: () = {
            use #crate_path::Operand as _Operand;
            use #crate_path::optimizer::PlanNode as _PlanNode;

            impl #impl_generics _PlanNode for #ident #type_generics #where_clause {
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
