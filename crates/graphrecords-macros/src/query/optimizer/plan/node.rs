use super::{PlanModel, with_bounds};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        generics,
        crate_path,
        inputs,
        arguments,
        argument_types,
        payload,
        ..
    } = PlanModel::parse(input)?;

    let generics = with_bounds(
        &generics,
        &argument_types,
        &quote!(#crate_path::optimizer::PlanInputs),
    );

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::optimizer::PlanNode for #ident #type_generics #where_clause {
            fn inputs(&self) -> ::std::vec::Vec<&dyn #crate_path::optimizer::PlanNode> {
                let mut nodes: ::std::vec::Vec<&dyn #crate_path::optimizer::PlanNode> =
                    ::std::vec![ #( #crate_path::Expression::as_plan_node(&self.#inputs), )* ];
                #(
                    nodes.extend(
                        #crate_path::optimizer::PlanInputs::inputs(&self.#arguments),
                    );
                )*

                nodes
            }

            fn dyn_eq(&self, other: &dyn #crate_path::optimizer::PlanNode) -> bool {
                let ::core::option::Option::Some(other) = other.downcast::<Self>() else {
                    return false;
                };

                #(
                    if self.#payload != other.#payload {
                        return false;
                    }
                )*

                #crate_path::optimizer::PlanNode::inputs(self)
                    .into_iter()
                    .zip(#crate_path::optimizer::PlanNode::inputs(other))
                    .all(|(self_input, other_input)| {
                        #crate_path::optimizer::PlanNode::dyn_eq(self_input, other_input)
                    })
            }

            fn dyn_hash(&self, mut state: &mut dyn ::core::hash::Hasher) {
                ::core::hash::Hash::hash(&::core::any::Any::type_id(self), &mut state);
                #( ::core::hash::Hash::hash(&self.#payload, &mut state); )*

                for input in #crate_path::optimizer::PlanNode::inputs(self) {
                    #crate_path::optimizer::PlanNode::dyn_hash(input, state);
                }
            }
        }
    })
}
