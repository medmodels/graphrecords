use crate::plan::PlanModel;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Error, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let PlanModel {
        ident,
        generics,
        crate_path,
        operand,
        inputs,
        payload,
        is_unit,
        ..
    } = PlanModel::parse(input)?;

    let Some(operand) = operand else {
        return Err(Error::new_spanned(
            input,
            "`OptimizeInputs` requires the operand, e.g. `#[plan(operand = ...)]`",
        ));
    };

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let optimized_locals: Vec<_> = (0..inputs.len())
        .map(|index| format_ident!("optimized_input_{index}"))
        .collect();

    let optimized_locals_changed = optimized_locals.clone();
    let optimized_locals_value = optimized_locals.clone();
    let inputs_to_optimize = inputs.clone();
    let inputs_to_rebuild = inputs;
    let rebuild_payload = payload;

    let body = if is_unit {
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

    Ok(quote! {
        impl #impl_generics #crate_path::optimizer::OptimizeInputs for #ident #type_generics #where_clause {
            type Output = #operand;

            fn optimize_inputs(
                &self,
                original: &Self::Output,
                session: &#crate_path::optimizer::Session,
            ) -> #crate_path::optimizer::Transformed<Self::Output> {
                #body
            }
        }
    })
}
