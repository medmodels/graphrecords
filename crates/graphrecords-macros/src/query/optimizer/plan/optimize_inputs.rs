use super::{PlanModel, with_bounds};
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
        arguments,
        argument_types,
        payload,
        is_unit,
        ..
    } = PlanModel::parse(input)?;

    let Some(operand) = operand else {
        return Err(Error::new_spanned(
            input,
            "`OptimizePlan` requires the operand, e.g. `#[plan(operand = ...)]`",
        ));
    };

    let generics = with_bounds(
        &generics,
        &argument_types,
        &quote!(#crate_path::optimizer::PlanInputs),
    );

    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    let inputs = &inputs;
    let arguments = &arguments;
    let payload = &payload;

    let body = if is_unit {
        quote! {
            #crate_path::optimizer::Transformed::unchanged(::core::clone::Clone::clone(original))
        }
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
            #( let #input_locals = session.optimize(&self.#inputs); )*
            #(
                let #argument_locals =
                    #crate_path::optimizer::PlanInputs::optimize(&self.#arguments, session);
            )*

            let changed = false
                #( || #input_locals.is_changed() )*
                #( || #argument_locals.is_changed() )*;

            if !changed {
                return #crate_path::optimizer::Transformed::unchanged(
                    ::core::clone::Clone::clone(original),
                );
            }

            #crate_path::optimizer::Transformed::changed(
                <#operand as #crate_path::Operand>::from_context(::std::sync::Arc::new(Self {
                    #( #inputs: #input_locals.into_parts().0, )*
                    #( #arguments: #argument_locals.into_parts().0, )*
                    #( #payload: ::core::clone::Clone::clone(&self.#payload), )*
                })),
            )
        }
    };

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::optimizer::OptimizePlan for #ident #type_generics #where_clause {
            type Output = #operand;

            fn optimize(
                &self,
                original: &Self::Output,
                session: &#crate_path::optimizer::Session,
            ) -> #crate_path::optimizer::Transformed<Self::Output> {
                #body
            }
        }
    })
}
