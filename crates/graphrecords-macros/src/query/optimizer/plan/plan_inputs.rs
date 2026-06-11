use super::{PlanModel, with_bounds};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
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

    let inputs = &inputs;
    let arguments = &arguments;
    let payload = &payload;

    let plan_inputs_method = if inputs.is_empty() && arguments.is_empty() {
        quote!()
    } else {
        quote! {
            fn inputs(&self) -> ::std::vec::Vec<&dyn #crate_path::optimizer::PlanNode> {
                let mut nodes: ::std::vec::Vec<&dyn #crate_path::optimizer::PlanNode> =
                    ::std::vec![ #( #crate_path::Operand::as_plan_node(&self.#inputs), )* ];
                #(
                    nodes.extend(
                        #crate_path::optimizer::PlanInputs::inputs(&self.#arguments),
                    );
                )*

                nodes
            }
        }
    };

    let optimize_method = if inputs.is_empty() && arguments.is_empty() {
        quote!()
    } else {
        let input_locals: Vec<_> = (0..inputs.len())
            .map(|index| format_ident!("optimized_input_{index}"))
            .collect();
        let input_locals = &input_locals;
        let argument_locals: Vec<_> = (0..arguments.len())
            .map(|index| format_ident!("optimized_argument_{index}"))
            .collect();
        let argument_locals = &argument_locals;

        quote! {
            fn optimize(
                &self,
                session: &#crate_path::optimizer::Session,
            ) -> #crate_path::optimizer::Transformed<Self> {
                #( let #input_locals = session.optimize(&self.#inputs); )*
                #(
                    let #argument_locals =
                        #crate_path::optimizer::PlanInputs::optimize(&self.#arguments, session);
                )*

                let changed = false
                    #( || #input_locals.changed )*
                    #( || #argument_locals.changed )*;

                if !changed {
                    return #crate_path::optimizer::Transformed::unchanged(
                        ::core::clone::Clone::clone(self),
                    );
                }

                #crate_path::optimizer::Transformed {
                    value: Self {
                        #( #inputs: #input_locals.value, )*
                        #( #arguments: #argument_locals.value, )*
                        #( #payload: ::core::clone::Clone::clone(&self.#payload), )*
                    },
                    changed: true,
                }
            }
        }
    };

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::optimizer::PlanInputs for #ident #type_generics #where_clause {
            #plan_inputs_method
            #optimize_method
        }
    })
}
